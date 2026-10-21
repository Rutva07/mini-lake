# Local acceptance benchmark

This record captures the reproducible acceptance run performed on October 20, 2026. It validates the optimization path; it is not a promise that every dataset or machine will produce the same latency.

## Setup

- Python 3.9.6
- Apache Arrow 19.0.1
- 20,000 deterministic ecommerce events (`seed=7`)
- 30 daily partitions × 4 event-type partitions = 120 Parquet files
- Snappy compression
- Filter: January 10–12, 2026 and `event_type=purchase`
- Five timed repeats after one warm-up scan

```bash
minilake generate-demo --output /tmp/minilake-acceptance/events.csv --rows 20000 --seed 7
minilake run \
  --input /tmp/minilake-acceptance/events.csv \
  --output /tmp/minilake-acceptance/lake \
  --run-id acceptance-test \
  --batch-size 4096 \
  --max-rows-per-file 5000
minilake benchmark \
  --dataset /tmp/minilake-acceptance/lake \
  --start-date 2026-01-10 \
  --end-date 2026-01-12 \
  --event-type purchase \
  --repeats 5
```

## Observed result

| Metric | Result |
|---|---:|
| Rows written | 20,000 |
| Rows matching | 185 |
| All files | 120 |
| Selected files | 3 |
| All bytes | 1,494,156 |
| Selected bytes | 31,160 |
| Scan reduction | 97.91% |
| Full-scan median | 1.438 ms |
| Filtered-scan median | 0.230 ms |
| Observed speedup | 6.26× |

For this acceptance dataset, both project targets were met: at least 3.2× faster filtered queries and at least 30% less data scanned. The absolute timings are intentionally reported with the data size and environment because a small cached local dataset is not representative of S3 or Snowflake latency. Re-run the same command against deployment-scale data before using the numbers externally.

