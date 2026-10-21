from typing import Any


REQUIRED_INPUT_COLUMNS = (
    "event_id",
    "user_id",
    "event_type",
    "event_ts",
    "amount",
    "country",
)


def event_schema() -> Any:
    """Return the canonical Arrow schema without importing Arrow at package import time."""
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("user_id", pa.string(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("country", pa.string(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("event_date", pa.date32(), nullable=False),
        ]
    )

