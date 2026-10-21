import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from minilake.config import PipelineConfig
from minilake.pipeline import run_pipeline
from minilake.query import query_events


ROWS = [
    {
        "event_id": "evt-1",
        "user_id": "usr-1",
        "event_type": "page_view",
        "event_ts": "2026-02-01T10:00:00Z",
        "amount": None,
        "country": "US",
    },
    {
        "event_id": "evt-2",
        "user_id": "usr-2",
        "event_type": "purchase",
        "event_ts": "2026-02-02T11:00:00Z",
        "amount": 19.95,
        "country": "CA",
    },
]


@pytest.mark.parametrize("file_format", ["jsonl", "parquet"])
def test_pipeline_accepts_supported_columnar_and_json_formats(tmp_path: Path, file_format: str) -> None:
    if file_format == "jsonl":
        source = tmp_path / "events.jsonl"
        source.write_text("\n".join(json.dumps(row) for row in ROWS) + "\n", encoding="utf-8")
    else:
        source = tmp_path / "events.parquet"
        pq.write_table(pa.Table.from_pylist(ROWS), source)

    output = tmp_path / "lake"
    manifest = run_pipeline(PipelineConfig(source, output), run_id=f"{file_format}-run")
    assert manifest.rows_written == 2
    result = query_events(output, event_type="purchase")
    assert result.num_rows == 1
    assert result["event_id"].to_pylist() == ["evt-2"]

