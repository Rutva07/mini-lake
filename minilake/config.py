from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from minilake.errors import ConfigurationError


SUPPORTED_FORMATS = {"csv", "json", "jsonl", "ndjson", "parquet"}
SUPPORTED_COMPRESSIONS = {"snappy", "gzip", "brotli", "zstd", "none"}


@dataclass(frozen=True)
class PipelineConfig:
    input_path: Path
    output_path: Path
    input_format: str = "auto"
    partition_by: Tuple[str, ...] = ("event_date", "event_type")
    compression: str = "snappy"
    batch_size: int = 65_536
    max_rows_per_file: int = 250_000
    source_name: str = "minilake"

    def validate(self) -> "PipelineConfig":
        if not self.input_path.exists():
            raise ConfigurationError(f"Input path does not exist: {self.input_path}")
        if self.input_format != "auto" and self.input_format not in SUPPORTED_FORMATS:
            raise ConfigurationError(f"Unsupported input format: {self.input_format}")
        if self.compression not in SUPPORTED_COMPRESSIONS:
            raise ConfigurationError(f"Unsupported compression: {self.compression}")
        if self.batch_size < 1 or self.max_rows_per_file < 1:
            raise ConfigurationError("Batch and file row limits must be positive")
        if not self.partition_by:
            raise ConfigurationError("At least one partition column is required")
        allowed_partitions = {"event_date", "event_type", "country"}
        unknown = set(self.partition_by) - allowed_partitions
        if unknown:
            raise ConfigurationError(f"Unsupported partition columns: {sorted(unknown)}")
        return self

