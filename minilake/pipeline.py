import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from minilake.config import PipelineConfig
from minilake.errors import ConfigurationError
from minilake.extract import discover_files, iter_record_batches
from minilake.manifest import RunManifest
from minilake.schema import event_schema
from minilake.transform import transform_batch


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def run_pipeline(config: PipelineConfig, run_id: Optional[str] = None) -> RunManifest:
    import pyarrow as pa
    import pyarrow.dataset as ds

    config.validate()
    identifier = run_id or new_run_id()
    if not identifier or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in identifier):
        raise ConfigurationError("run_id may contain only letters, digits, hyphens, and underscores")

    files = discover_files(config.input_path, config.input_format)
    config.output_path.mkdir(parents=True, exist_ok=True)
    manifest_path = config.output_path / "_manifests" / f"{identifier}.json"
    if manifest_path.exists():
        raise ConfigurationError(f"Run id already exists: {identifier}")

    staging = config.output_path / ".staging" / f"{identifier}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True)
    rows_read = 0
    output_paths = []
    ingestion_time = datetime.now(timezone.utc)
    compression = None if config.compression == "none" else config.compression

    parquet_format = ds.ParquetFileFormat()
    write_options = parquet_format.make_write_options(
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
    )

    try:
        def normalized_batches():
            nonlocal rows_read
            for _, batch in iter_record_batches(files, config.batch_size):
                normalized = transform_batch(batch, config.source_name, ingestion_time)
                rows_read += normalized.num_rows
                yield from normalized.to_batches()

        reader = pa.RecordBatchReader.from_batches(event_schema(), normalized_batches())
        ds.write_dataset(
            reader,
            base_dir=str(staging),
            format=parquet_format,
            file_options=write_options,
            partitioning=list(config.partition_by),
            partitioning_flavor="hive",
            basename_template=f"{identifier}-{{i}}.parquet",
            max_rows_per_file=config.max_rows_per_file,
            max_rows_per_group=min(64_000, config.max_rows_per_file),
            existing_data_behavior="error",
        )

        if rows_read == 0:
            raise ConfigurationError("No rows were found in the input")

        for source in sorted(staging.rglob("*.parquet")):
            relative = source.relative_to(staging)
            destination = config.output_path / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                raise ConfigurationError(f"Output collision: {destination}")
            os.replace(source, destination)
            output_paths.append(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
        staging_parent = config.output_path / ".staging"
        if staging_parent.exists() and not any(staging_parent.iterdir()):
            staging_parent.rmdir()

    manifest = RunManifest.create(
        run_id=identifier,
        input_files=[str(path) for path, _ in files],
        rows_read=rows_read,
        output_root=config.output_path,
        output_paths=output_paths,
        partition_by=list(config.partition_by),
        compression=config.compression,
        schema=str(event_schema()),
    )
    manifest.write(config.output_path)
    return manifest

