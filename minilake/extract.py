from pathlib import Path
from typing import Iterator, List, Sequence, Tuple

from minilake.errors import ConfigurationError
from minilake.schema import REQUIRED_INPUT_COLUMNS


EXTENSION_FORMATS = {
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
    ".parquet": "parquet",
}


def discover_files(path: Path, input_format: str = "auto") -> List[Tuple[Path, str]]:
    candidates = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
    discovered = []
    for candidate in candidates:
        detected = EXTENSION_FORMATS.get(candidate.suffix.lower())
        requested = "json" if input_format in {"jsonl", "ndjson"} else input_format
        if requested == "auto" and detected:
            discovered.append((candidate, detected))
        elif requested != "auto" and detected == requested:
            discovered.append((candidate, requested))
    if not discovered:
        raise ConfigurationError(f"No supported input files found under {path}")
    return discovered


def iter_record_batches(
    files: Sequence[Tuple[Path, str]], batch_size: int
) -> Iterator[Tuple[Path, object]]:
    import pyarrow as pa
    import pyarrow.csv as csv
    import pyarrow.json as json
    import pyarrow.parquet as pq

    for path, file_format in files:
        if file_format == "csv":
            # Read source fields as strings so normalization owns all type conversion.
            convert = csv.ConvertOptions(column_types={name: pa.string() for name in REQUIRED_INPUT_COLUMNS})
            reader = csv.open_csv(
                path,
                read_options=csv.ReadOptions(block_size=max(1 << 16, batch_size * 128)),
                convert_options=convert,
            )
            for batch in reader:
                # Arrow's CSV block size is byte-oriented. Re-slice record batches
                # so the public row-oriented batch_size contract is still honored.
                for chunk in pa.Table.from_batches([batch]).to_batches(max_chunksize=batch_size):
                    yield path, chunk
        elif file_format == "json":
            table = json.read_json(path, read_options=json.ReadOptions(block_size=max(1 << 20, batch_size * 128)))
            for batch in table.to_batches(max_chunksize=batch_size):
                yield path, batch
        elif file_format == "parquet":
            parquet_file = pq.ParquetFile(path)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                yield path, batch
        else:  # defensive guard; discover_files prevents this path.
            raise ConfigurationError(f"Unsupported format {file_format!r} for {path}")

