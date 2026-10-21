import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from minilake import __version__
from minilake.errors import MiniLakeError, OptionalDependencyError


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str))


def _pipeline_config(args: argparse.Namespace) -> object:
    from minilake.config import PipelineConfig

    partitions = tuple(item.strip() for item in args.partition_by.split(",") if item.strip())
    return PipelineConfig(
        input_path=Path(args.input),
        output_path=Path(args.output),
        input_format=args.input_format,
        partition_by=partitions,
        compression=args.compression,
        batch_size=args.batch_size,
        max_rows_per_file=args.max_rows_per_file,
        source_name=args.source,
    )


def command_generate_demo(args: argparse.Namespace) -> None:
    from minilake.demo import generate_demo

    path = generate_demo(Path(args.output), args.rows, args.seed)
    _print_json({"path": str(path), "rows": args.rows, "seed": args.seed})


def command_run(args: argparse.Namespace) -> None:
    from minilake.pipeline import run_pipeline

    manifest = run_pipeline(_pipeline_config(args), args.run_id)
    _print_json(manifest.to_dict())


def command_query(args: argparse.Namespace) -> None:
    from minilake.query import query_events

    columns = [item.strip() for item in args.columns.split(",") if item.strip()] if args.columns else None
    table = query_events(
        Path(args.dataset),
        start_date=args.start_date,
        end_date=args.end_date,
        event_type=args.event_type,
        country=args.country,
        columns=columns,
    )
    preview = table.slice(0, args.limit).to_pylist()
    _print_json({"rows_matching": table.num_rows, "rows_returned": len(preview), "data": preview})


def command_benchmark(args: argparse.Namespace) -> None:
    from minilake.query import benchmark_query

    result = benchmark_query(
        Path(args.dataset),
        start_date=args.start_date,
        end_date=args.end_date,
        event_type=args.event_type,
        repeats=args.repeats,
    )
    _print_json(result.to_dict())


def command_upload_s3(args: argparse.Namespace) -> None:
    from minilake.cloud.s3 import S3Uploader

    uploader = S3Uploader(region=args.region)
    uploaded = uploader.upload_tree(Path(args.source), args.bucket, args.prefix)
    _print_json(
        {
            "files_uploaded": len(uploaded),
            "bytes_uploaded": sum(int(item["size_bytes"]) for item in uploaded),
            "destination": f"s3://{args.bucket}/{args.prefix.strip('/')}",
            "files": uploaded,
        }
    )


def _snowflake_value(args: argparse.Namespace, name: str, fallback: Optional[str] = None) -> str:
    value = getattr(args, name, None) or os.getenv(f"SNOWFLAKE_{name.upper()}") or fallback
    if not value:
        raise MiniLakeError(f"Provide --{name.replace('_', '-')} or SNOWFLAKE_{name.upper()}")
    return value


def command_snowflake_init(args: argparse.Namespace) -> None:
    from minilake.cloud.snowflake import SnowflakeClient, SnowflakeSettings, bootstrap_statements

    settings = SnowflakeSettings.from_env()
    statements = bootstrap_statements(
        warehouse=_snowflake_value(args, "warehouse", settings.warehouse),
        database=_snowflake_value(args, "database", settings.database),
        schema=_snowflake_value(args, "schema", settings.schema),
        stage=args.stage,
        s3_url=args.s3_url,
        storage_integration=args.storage_integration,
    )
    client = SnowflakeClient(settings)
    try:
        _print_json({"queries": client.execute(statements)})
    finally:
        client.close()


def command_snowflake_load(args: argparse.Namespace) -> None:
    from minilake.cloud.snowflake import SnowflakeClient, SnowflakeSettings, copy_statement

    settings = SnowflakeSettings.from_env()
    statement = copy_statement(
        database=_snowflake_value(args, "database", settings.database),
        schema=_snowflake_value(args, "schema", settings.schema),
        table=args.table,
        stage=args.stage,
    )
    client = SnowflakeClient(settings)
    try:
        _print_json({"queries": client.execute([statement])})
    finally:
        client.close()


def command_clean(args: argparse.Namespace) -> None:
    path = Path(args.path).resolve()
    if not path.exists():
        _print_json({"path": str(path), "files_removed": 0})
        return
    removable = [
        item
        for item in path.rglob("*")
        if item.is_file() and (item.suffix == ".parquet" or "_manifests" in item.parts)
    ]
    for item in removable:
        item.unlink()
    for directory in sorted((item for item in path.rglob("*") if item.is_dir()), reverse=True):
        if not any(directory.iterdir()):
            directory.rmdir()
    _print_json({"path": str(path), "files_removed": len(removable)})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minilake",
        description="Build and query a partitioned Parquet lakehouse, then publish it to S3/Snowflake.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    demo = commands.add_parser("generate-demo", help="Generate deterministic ecommerce events as CSV")
    demo.add_argument("--output", default="data/raw/events.csv")
    demo.add_argument("--rows", type=int, default=50_000)
    demo.add_argument("--seed", type=int, default=7)
    demo.set_defaults(handler=command_generate_demo)

    run = commands.add_parser("run", help="Normalize and partition source events as Parquet")
    run.add_argument("--input", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--input-format", choices=("auto", "csv", "json", "jsonl", "ndjson", "parquet"), default="auto")
    run.add_argument("--partition-by", default="event_date,event_type")
    run.add_argument("--compression", choices=("snappy", "gzip", "brotli", "zstd", "none"), default="snappy")
    run.add_argument("--batch-size", type=int, default=65_536)
    run.add_argument("--max-rows-per-file", type=int, default=250_000)
    run.add_argument("--source", default="minilake")
    run.add_argument("--run-id")
    run.set_defaults(handler=command_run)

    query = commands.add_parser("query", help="Query the local Arrow dataset with pushdown filters")
    query.add_argument("--dataset", required=True)
    query.add_argument("--start-date")
    query.add_argument("--end-date")
    query.add_argument("--event-type")
    query.add_argument("--country")
    query.add_argument("--columns")
    query.add_argument("--limit", type=int, default=20)
    query.set_defaults(handler=command_query)

    benchmark = commands.add_parser("benchmark", help="Measure partition pruning and filtered scan latency")
    benchmark.add_argument("--dataset", required=True)
    benchmark.add_argument("--start-date")
    benchmark.add_argument("--end-date")
    benchmark.add_argument("--event-type", default="purchase")
    benchmark.add_argument("--repeats", type=int, default=5)
    benchmark.set_defaults(handler=command_benchmark)

    upload = commands.add_parser("upload-s3", help="Upload a local lakehouse tree to Amazon S3")
    upload.add_argument("--source", required=True)
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--prefix", default="minilake/events")
    upload.add_argument("--region", default=os.getenv("AWS_REGION"))
    upload.set_defaults(handler=command_upload_s3)

    sf_init = commands.add_parser("snowflake-init", help="Create Snowflake warehouse, schema, stage, and table")
    sf_init.add_argument("--warehouse")
    sf_init.add_argument("--database")
    sf_init.add_argument("--schema")
    sf_init.add_argument("--stage", default="MINILAKE_STAGE")
    sf_init.add_argument("--s3-url", required=True)
    sf_init.add_argument("--storage-integration", required=True)
    sf_init.set_defaults(handler=command_snowflake_init)

    sf_load = commands.add_parser("snowflake-load", help="COPY partitioned Parquet from the external stage")
    sf_load.add_argument("--database")
    sf_load.add_argument("--schema")
    sf_load.add_argument("--table", default="EVENTS")
    sf_load.add_argument("--stage", default="MINILAKE_STAGE")
    sf_load.set_defaults(handler=command_snowflake_load)

    clean = commands.add_parser("clean", help="Remove generated Parquet files and manifests")
    clean.add_argument("--path", required=True)
    clean.set_defaults(handler=command_clean)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.startswith("pyarrow"):
            error = OptionalDependencyError("Apache Arrow is not installed. Run: pip install -e '.[dev]'")
            print(f"error: {error}", file=sys.stderr)
            raise SystemExit(2) from exc
        raise
    except MiniLakeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()

