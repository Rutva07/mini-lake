import json
from datetime import date
from pathlib import Path

from minilake.config import PipelineConfig
from minilake.demo import generate_demo
from minilake.pipeline import run_pipeline
from minilake.query import benchmark_query, query_events


def test_end_to_end_pipeline_and_query(tmp_path: Path) -> None:
    source = generate_demo(tmp_path / "raw" / "events.csv", rows=1_000, seed=10)
    output = tmp_path / "lake"
    manifest = run_pipeline(
        PipelineConfig(source, output, batch_size=128, max_rows_per_file=250),
        run_id="test-run",
    )

    assert manifest.rows_read == 1_000
    assert manifest.rows_written == 1_000
    assert all(Path(item.path).name.startswith("test-run-") for item in manifest.output_files)
    assert manifest.output_files
    assert all(len(item.sha256) == 64 for item in manifest.output_files)
    assert list(output.glob("event_date=*/event_type=*/*.parquet"))

    saved = json.loads((output / "_manifests" / "test-run.json").read_text(encoding="utf-8"))
    assert saved["rows_written"] == 1_000

    purchases = query_events(output, event_type="purchase")
    assert 0 < purchases.num_rows < 1_000
    assert set(purchases["event_type"].to_pylist()) == {"purchase"}

    dated = query_events(output, start_date="2026-01-10", end_date="2026-01-12")
    assert dated.num_rows > 0
    assert all(date(2026, 1, 10) <= value <= date(2026, 1, 12) for value in dated["event_date"].to_pylist())

    benchmark = benchmark_query(output, event_type="purchase", repeats=2)
    assert benchmark.selected_files < benchmark.total_files
    assert benchmark.selected_bytes < benchmark.total_bytes
    assert benchmark.scan_reduction_percent > 0


def test_pipeline_is_idempotent_by_run_id(tmp_path: Path) -> None:
    source = generate_demo(tmp_path / "events.csv", rows=10)
    output = tmp_path / "lake"
    config = PipelineConfig(source, output)
    run_pipeline(config, run_id="same-run")
    try:
        run_pipeline(config, run_id="same-run")
    except Exception as exc:
        assert "Run id already exists" in str(exc)
    else:
        raise AssertionError("Expected duplicate run id to fail")

