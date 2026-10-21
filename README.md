# MiniLake: Cloud Data Lakehouse

MiniLake is a compact, production-shaped ETL project that converts raw ecommerce events into an analytics-ready lakehouse. It streams CSV, newline-delimited JSON, or Parquet through Apache Arrow; enforces a canonical schema; writes compressed, Hive-partitioned Parquet; queries the lake locally with predicate pushdown; uploads it to Amazon S3; and loads it into Snowflake.

The repository runs end to end on a laptop without Docker, AWS credentials, or a Snowflake account. Cloud dependencies and credentials are only required for the publishing steps.

## What it demonstrates

- Batch-oriented ETL with Apache Arrow rather than loading an entire CSV into Python objects at once
- Type normalization and a strict event data contract
- Snappy-compressed Parquet with dictionary encoding and column statistics
- Hive partitions by `event_date` and `event_type`
- Predicate pushdown, column projection, and partition pruning
- Atomic staging, collision checks, repeatable run IDs, SHA-256 checksums, and JSON manifests
- Concurrent multipart S3 uploads through boto3
- Safe, validated Snowflake identifiers and an external-stage `COPY INTO` workflow
- Unit, integration, cloud-adapter, and SQL-generation tests

## Architecture

```text
CSV / JSONL / Parquet
        │
        ▼
Arrow batch reader ──► schema validation + normalization
        │
        ▼
Snappy Parquet writer
        │
        ├──► event_date=YYYY-MM-DD/event_type=.../*.parquet
        └──► _manifests/<run-id>.json
                         │
                ┌────────┴────────┐
                ▼                 ▼
         Arrow local query     Amazon S3
                                  │
                                  ▼
                         Snowflake external stage
                                  │
                                  ▼
                         clustered EVENTS table
```

## Event contract

Raw input must contain these columns:

| Column | Input | Canonical type | Rule |
|---|---|---|---|
| `event_id` | string | string | Required, trimmed |
| `user_id` | string | string | Required, trimmed |
| `event_type` | string | string | Required, lowercase |
| `event_ts` | ISO-8601 string/timestamp | UTC timestamp | Required; naive values are treated as UTC |
| `amount` | number/string/null | double | Optional |
| `country` | string | string | Required, uppercase |

The pipeline adds `source`, `ingested_at`, and `event_date`.

## Quick start

Python 3.9 or newer is supported.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e '.[dev]'

# Generate deterministic sample data.
minilake generate-demo --output data/raw/events.csv --rows 50000

# Run the ETL pipeline.
minilake run \
  --input data/raw/events.csv \
  --output data/lake \
  --source ecommerce-api

# Query with partition and row-group filters.
minilake query \
  --dataset data/lake \
  --start-date 2026-01-10 \
  --end-date 2026-01-12 \
  --event-type purchase \
  --columns event_id,user_id,event_ts,amount,country,event_date,event_type \
  --limit 5

# Compare an unfiltered scan with a partition-pruned scan.
minilake benchmark \
  --dataset data/lake \
  --start-date 2026-01-10 \
  --end-date 2026-01-12 \
  --event-type purchase
```

`make demo` runs the same local workflow. Generated data is ignored by Git.

### Example output layout

```text
data/lake/
├── _manifests/
│   └── 20261020T120000Z-a1b2c3d4.json
├── event_date=2026-01-01/
│   ├── event_type=page_view/*.parquet
│   └── event_type=purchase/*.parquet
└── event_date=2026-01-02/
    └── event_type=.../*.parquet
```

Every manifest records source files, row counts, output byte counts, the Arrow schema, and the SHA-256 checksum of each published Parquet file. A supplied `--run-id` cannot be reused, preventing an accidental duplicate publish.

## Performance design

MiniLake uses two complementary optimizations:

1. **Partition pruning**: date and event-type filters remove unrelated files before they are opened.
2. **Predicate pushdown**: Arrow uses Parquet metadata and row-group statistics to avoid decoding irrelevant data inside selected files. Column projection further limits IO.

The benchmark reports median wall-clock latency, selected files, selected bytes, and scan reduction. Results depend on data distribution, storage, cache state, and hardware; do not claim a fixed speedup without recording the output from the target environment. The deterministic generator makes before/after measurements reproducible with the same seed.

The checked local acceptance run achieved a 6.26× median speedup and 97.91% scan reduction on 20,000 generated rows; methodology and caveats are recorded in [`docs/BENCHMARK.md`](docs/BENCHMARK.md).

## Input formats and operational options

`minilake run` auto-detects `.csv`, `.json`, `.jsonl`, `.ndjson`, and `.parquet` files. A directory input recursively discovers all supported files.

```bash
minilake run --help
```

Useful options include:

- `--partition-by event_date,event_type` (also supports `country`)
- `--compression snappy|gzip|brotli|zstd|none`
- `--batch-size 65536`
- `--max-rows-per-file 250000`
- `--run-id my-backfill-2026-01`

Choose partition columns with bounded cardinality. Partitioning by `user_id` or `event_id` would create many tiny files, so MiniLake intentionally disallows it.

## Publish to Amazon S3

Install the optional AWS dependency and authenticate through any standard boto3 mechanism (AWS profile, environment variables, web identity, or instance role):

```bash
python3 -m pip install -e '.[aws]'
aws sts get-caller-identity  # optional credential check if the AWS CLI is installed

minilake upload-s3 \
  --source data/lake \
  --bucket my-analytics-bucket \
  --prefix minilake/events \
  --region us-east-1
```

The principal needs `s3:PutObject` for the target prefix and, depending on the credential setup, may also need `s3:ListBucket`. The uploader preserves Hive partition keys and includes manifests.

## Load into Snowflake

Install the connector:

```bash
python3 -m pip install -e '.[snowflake]'
cp .env.example .env
```

Export the values from `.env` in your shell. The password is never read from command-line flags, which keeps it out of shell history. A Snowflake administrator must first create a storage integration that can read the S3 prefix.

Create the X-Small auto-suspending warehouse, database, schema, Parquet file format, external stage, and clustered table:

```bash
minilake snowflake-init \
  --warehouse MINILAKE_WH \
  --database MINILAKE \
  --schema ANALYTICS \
  --stage MINILAKE_STAGE \
  --s3-url s3://my-analytics-bucket/minilake/events \
  --storage-integration MY_S3_INTEGRATION
```

Load the Parquet files:

```bash
minilake snowflake-load \
  --database MINILAKE \
  --schema ANALYTICS \
  --table EVENTS \
  --stage MINILAKE_STAGE
```

Hive partition columns do not live inside the Parquet payload. The generated `COPY INTO` statement extracts them from `METADATA$FILENAME`, so the local and Snowflake schemas remain identical. Example analytics are in [`sql/analytics.sql`](sql/analytics.sql).

## Testing

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest -q
```

The end-to-end test generates 1,000 events, runs ETL in small batches, validates the manifest and checksums, reads the dataset back, applies a partition filter, and verifies that the benchmark selects fewer files and bytes. S3 is tested with an in-memory fake client; Snowflake SQL is validated without making a network connection.

Cloud execution is intentionally not part of the default test suite because it would require billable accounts and credentials. Run the S3 and Snowflake commands in a provisioned environment for the final infrastructure acceptance test.

## Repository map

```text
minilake/
├── cli.py                # CLI entry points
├── config.py             # validated pipeline configuration
├── demo.py               # deterministic data generator
├── extract.py            # CSV/JSON/Parquet Arrow readers
├── transform.py          # schema and normalization contract
├── pipeline.py           # partitioned writer + atomic publish
├── query.py              # filters, projection, and benchmark
├── manifest.py           # checksums and run metadata
└── cloud/
    ├── s3.py             # multipart S3 publisher
    └── snowflake.py      # DDL and COPY workflow
tests/                    # unit and end-to-end tests
sql/analytics.sql         # example warehouse queries
```

## License

MIT

