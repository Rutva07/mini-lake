from pathlib import Path

import pytest

from minilake.cloud.s3 import S3Uploader
from minilake.cloud.snowflake import bootstrap_statements, copy_statement, quote_identifier
from minilake.errors import ConfigurationError


class FakeS3Client:
    def __init__(self) -> None:
        self.calls = []

    def upload_file(self, *args: object, **kwargs: object) -> None:
        self.calls.append((args, kwargs))


def test_s3_upload_preserves_partition_keys(tmp_path: Path) -> None:
    parquet = tmp_path / "event_date=2026-01-01" / "event_type=purchase" / "part.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"PAR1")
    client = FakeS3Client()
    result = S3Uploader(client=client).upload_tree(tmp_path, "bucket", "lake/events")
    assert result[0]["uri"] == "s3://bucket/lake/events/event_date=2026-01-01/event_type=purchase/part.parquet"
    assert client.calls[0][0][2] == "lake/events/event_date=2026-01-01/event_type=purchase/part.parquet"


def test_snowflake_sql_is_partition_aware() -> None:
    statements = bootstrap_statements("wh", "db", "analytics", "stage", "s3://bucket/events", "s3_int")
    assert any("CREATE STAGE" in statement for statement in statements)
    sql = copy_statement("db", "analytics", "events", "stage")
    assert "event_date=([^/]+)" in sql
    assert "event_type=([^/]+)" in sql
    assert "ON_ERROR = 'ABORT_STATEMENT'" in sql


def test_identifier_validation_blocks_sql_injection() -> None:
    with pytest.raises(ConfigurationError):
        quote_identifier("events; DROP TABLE users")

