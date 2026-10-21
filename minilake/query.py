import statistics
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from minilake.errors import ConfigurationError


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigurationError(f"Expected ISO date (YYYY-MM-DD), got {value!r}") from exc


def build_filter(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    event_type: Optional[str] = None,
    country: Optional[str] = None,
) -> object:
    import pyarrow.dataset as ds

    predicates = []
    parsed_start = _parse_date(start_date)
    parsed_end = _parse_date(end_date)
    if parsed_start:
        predicates.append(ds.field("event_date") >= parsed_start)
    if parsed_end:
        predicates.append(ds.field("event_date") <= parsed_end)
    if event_type:
        predicates.append(ds.field("event_type") == event_type.strip().lower())
    if country:
        predicates.append(ds.field("country") == country.strip().upper())
    if not predicates:
        return None
    expression = predicates[0]
    for predicate in predicates[1:]:
        expression = expression & predicate
    return expression


def open_dataset(path: Path) -> object:
    import pyarrow as pa
    import pyarrow.dataset as ds

    if not path.exists():
        raise ConfigurationError(f"Dataset does not exist: {path}")
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise ConfigurationError(f"No Parquet files found under {path}")
    partition_names = set()
    for parquet_file in parquet_files:
        for part in parquet_file.relative_to(path).parts[:-1]:
            if "=" in part:
                partition_names.add(part.split("=", 1)[0])
    canonical_types = {
        "event_date": pa.date32(),
        "event_type": pa.string(),
        "country": pa.string(),
    }
    unknown = partition_names - set(canonical_types)
    if unknown:
        raise ConfigurationError(f"Unknown Hive partition columns: {sorted(unknown)}")
    ordered_names = [name for name in ("event_date", "event_type", "country") if name in partition_names]
    partitioning = ds.partitioning(
        pa.schema([pa.field(name, canonical_types[name]) for name in ordered_names]),
        flavor="hive",
    )
    return ds.dataset(
        path.resolve(),
        format="parquet",
        partitioning=partitioning,
        exclude_invalid_files=True,
    )


def query_events(
    path: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    event_type: Optional[str] = None,
    country: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
) -> object:
    dataset = open_dataset(path)
    requested = list(columns) if columns else None
    if requested:
        unknown = set(requested) - set(dataset.schema.names)
        if unknown:
            raise ConfigurationError(f"Unknown projection columns: {sorted(unknown)}")
    return dataset.to_table(
        columns=requested,
        filter=build_filter(start_date, end_date, event_type, country),
    )


def _fragment_sizes(dataset: object, expression: object) -> Tuple[int, int]:
    all_fragments = list(dataset.get_fragments())
    selected_fragments = list(dataset.get_fragments(filter=expression)) if expression is not None else all_fragments

    def total_bytes(fragments: List[object]) -> int:
        total = 0
        for fragment in fragments:
            try:
                total += Path(fragment.path).stat().st_size
            except OSError:
                # Fragment paths may be relative to the dataset root.
                total += 0
        return total

    return total_bytes(all_fragments), total_bytes(selected_fragments)


@dataclass(frozen=True)
class BenchmarkResult:
    full_scan_median_ms: float
    filtered_scan_median_ms: float
    speedup: float
    total_files: int
    selected_files: int
    total_bytes: int
    selected_bytes: int
    scan_reduction_percent: float
    rows_matching: int
    repeats: int

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def benchmark_query(
    path: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    event_type: Optional[str] = None,
    repeats: int = 5,
) -> BenchmarkResult:
    if repeats < 1:
        raise ConfigurationError("repeats must be positive")
    dataset = open_dataset(path)
    expression = build_filter(start_date, end_date, event_type)
    if expression is None:
        raise ConfigurationError("Benchmark needs at least one date or event-type filter")

    # Warm both code paths once, then alternate them to reduce cache-order bias.
    dataset.scanner(columns=["event_id"]).count_rows()
    dataset.scanner(columns=["event_id"], filter=expression).count_rows()
    full_times = []
    filtered_times = []
    matching_rows = 0
    for _ in range(repeats):
        started = time.perf_counter()
        dataset.scanner(columns=["event_id"]).count_rows()
        full_times.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        matching_rows = dataset.scanner(columns=["event_id"], filter=expression).count_rows()
        filtered_times.append((time.perf_counter() - started) * 1000)

    all_fragments = list(dataset.get_fragments())
    selected_fragments = list(dataset.get_fragments(filter=expression))

    def fragment_bytes(fragment: object) -> int:
        fragment_path = Path(fragment.path)
        if not fragment_path.is_absolute():
            fragment_path = path / fragment_path
        try:
            return fragment_path.stat().st_size
        except OSError:
            return 0

    total_bytes = sum(fragment_bytes(fragment) for fragment in all_fragments)
    selected_bytes = sum(fragment_bytes(fragment) for fragment in selected_fragments)
    full_ms = statistics.median(full_times)
    filtered_ms = statistics.median(filtered_times)
    return BenchmarkResult(
        full_scan_median_ms=round(full_ms, 3),
        filtered_scan_median_ms=round(filtered_ms, 3),
        speedup=round(full_ms / filtered_ms, 2) if filtered_ms else float("inf"),
        total_files=len(all_fragments),
        selected_files=len(selected_fragments),
        total_bytes=total_bytes,
        selected_bytes=selected_bytes,
        scan_reduction_percent=round((1 - selected_bytes / total_bytes) * 100, 2) if total_bytes else 0.0,
        rows_matching=matching_rows,
        repeats=repeats,
    )

