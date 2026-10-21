from datetime import date

import pyarrow as pa
import pytest

from minilake.errors import DataValidationError
from minilake.transform import transform_batch


def source_batch(**overrides: object) -> pa.RecordBatch:
    row = {
        "event_id": " evt-1 ",
        "user_id": "user-1",
        "event_type": " Purchase ",
        "event_ts": "2026-01-02T03:04:05Z",
        "amount": "42.50",
        "country": "us",
    }
    row.update(overrides)
    return pa.Table.from_pylist([row]).to_batches()[0]


def test_transform_normalizes_event() -> None:
    table = transform_batch(source_batch(), "unit-test")
    row = table.to_pylist()[0]
    assert row["event_id"] == "evt-1"
    assert row["event_type"] == "purchase"
    assert row["country"] == "US"
    assert row["amount"] == 42.5
    assert row["event_date"] == date(2026, 1, 2)
    assert row["source"] == "unit-test"


def test_transform_rejects_bad_timestamp() -> None:
    with pytest.raises(DataValidationError, match="Invalid ISO-8601"):
        transform_batch(source_batch(event_ts="not-a-date"), "unit-test")


def test_transform_rejects_missing_columns() -> None:
    batch = pa.Table.from_pylist([{"event_id": "1"}]).to_batches()[0]
    with pytest.raises(DataValidationError, match="Missing required columns"):
        transform_batch(batch, "unit-test")

