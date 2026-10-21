from datetime import datetime, timezone
from typing import List, Optional

from minilake.errors import DataValidationError
from minilake.schema import REQUIRED_INPUT_COLUMNS, event_schema


def _parse_timestamp(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise DataValidationError(f"Invalid ISO-8601 event_ts: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _required_text(value: object, field: str, row_number: int) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        raise DataValidationError(f"Row {row_number}: {field} is required")
    return text


def transform_batch(batch: object, source_name: str, ingested_at: Optional[datetime] = None) -> object:
    """Normalize one Arrow batch to the canonical event schema.

    Arrow remains the IO and storage boundary. The explicit row contract provides
    deterministic validation messages and safely handles mixed JSON/CSV types.
    """
    import pyarrow as pa

    table = pa.Table.from_batches([batch])
    missing = set(REQUIRED_INPUT_COLUMNS) - set(table.column_names)
    if missing:
        raise DataValidationError(f"Missing required columns: {sorted(missing)}")

    ingestion_time = ingested_at or datetime.now(timezone.utc)
    rows: List[dict] = []
    for row_number, source_row in enumerate(table.select(list(REQUIRED_INPUT_COLUMNS)).to_pylist(), start=1):
        timestamp = _parse_timestamp(source_row["event_ts"])
        if timestamp is None:
            raise DataValidationError(f"Row {row_number}: event_ts is required")
        raw_amount = source_row["amount"]
        try:
            amount = None if raw_amount in (None, "") else float(raw_amount)
        except (TypeError, ValueError) as exc:
            raise DataValidationError(f"Row {row_number}: invalid amount {raw_amount!r}") from exc
        rows.append(
            {
                "event_id": _required_text(source_row["event_id"], "event_id", row_number),
                "user_id": _required_text(source_row["user_id"], "user_id", row_number),
                "event_type": _required_text(source_row["event_type"], "event_type", row_number).lower(),
                "event_ts": timestamp,
                "amount": amount,
                "country": _required_text(source_row["country"], "country", row_number).upper(),
                "source": source_name,
                "ingested_at": ingestion_time,
                "event_date": timestamp.date(),
            }
        )
    return pa.Table.from_pylist(rows, schema=event_schema())

