import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from minilake.errors import ConfigurationError, OptionalDependencyError


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def quote_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ConfigurationError(f"Unsafe Snowflake identifier: {value!r}")
    return f'"{value.upper()}"'


def _qualified(*parts: str) -> str:
    return ".".join(quote_identifier(part) for part in parts)


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None

    @classmethod
    def from_env(cls) -> "SnowflakeSettings":
        required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ConfigurationError(f"Missing Snowflake environment variables: {', '.join(missing)}")
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )


def bootstrap_statements(
    warehouse: str,
    database: str,
    schema: str,
    stage: str,
    s3_url: str,
    storage_integration: str,
) -> List[str]:
    if not s3_url.startswith("s3://"):
        raise ConfigurationError("Snowflake stage URL must start with s3://")
    wh = quote_identifier(warehouse)
    db = quote_identifier(database)
    namespace = _qualified(database, schema)
    stage_name = _qualified(database, schema, stage)
    table_name = _qualified(database, schema, "EVENTS")
    integration = quote_identifier(storage_integration)
    escaped_url = s3_url.rstrip("/").replace("'", "''")
    return [
        f"CREATE WAREHOUSE IF NOT EXISTS {wh} WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE",
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"CREATE SCHEMA IF NOT EXISTS {namespace}",
        f"CREATE FILE FORMAT IF NOT EXISTS {namespace}.\"MINILAKE_PARQUET\" TYPE = PARQUET",
        (
            f"CREATE STAGE IF NOT EXISTS {stage_name} URL = '{escaped_url}' "
            f"STORAGE_INTEGRATION = {integration} FILE_FORMAT = {namespace}.\"MINILAKE_PARQUET\""
        ),
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING NOT NULL,
            user_id STRING NOT NULL,
            event_ts TIMESTAMP_TZ NOT NULL,
            amount FLOAT,
            country STRING NOT NULL,
            source STRING NOT NULL,
            ingested_at TIMESTAMP_TZ NOT NULL,
            event_date DATE NOT NULL,
            event_type STRING NOT NULL
        ) CLUSTER BY (event_date, event_type)""",
    ]


def copy_statement(database: str, schema: str, table: str, stage: str) -> str:
    target = _qualified(database, schema, table)
    source = _qualified(database, schema, stage)
    # Hive partition values are encoded in each S3 key and deliberately omitted
    # from Parquet payloads; extract them from METADATA$FILENAME while loading.
    return f"""COPY INTO {target}
    (event_id, user_id, event_ts, amount, country, source, ingested_at, event_date, event_type)
FROM (
    SELECT
        $1:event_id::STRING,
        $1:user_id::STRING,
        $1:event_ts::TIMESTAMP_TZ,
        $1:amount::FLOAT,
        $1:country::STRING,
        $1:source::STRING,
        $1:ingested_at::TIMESTAMP_TZ,
        TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'event_date=([^/]+)', 1, 1, 'e', 1)),
        REGEXP_SUBSTR(METADATA$FILENAME, 'event_type=([^/]+)', 1, 1, 'e', 1)
    FROM @{source}
)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT'"""


class SnowflakeClient:
    def __init__(self, settings: SnowflakeSettings, connection: object = None) -> None:
        self.settings = settings
        if connection is not None:
            self.connection = connection
            return
        try:
            import snowflake.connector
        except ImportError as exc:
            raise OptionalDependencyError(
                "Snowflake support is not installed. Run: pip install -e '.[snowflake]'"
            ) from exc
        connect_args: Dict[str, str] = {
            "account": settings.account,
            "user": settings.user,
            "password": settings.password,
        }
        for key in ("role", "warehouse", "database", "schema"):
            value = getattr(settings, key)
            if value:
                connect_args[key] = value
        self.connection = snowflake.connector.connect(**connect_args)

    def execute(self, statements: List[str]) -> List[Dict[str, object]]:
        results = []
        cursor = self.connection.cursor()
        try:
            for statement in statements:
                cursor.execute(statement)
                results.append(
                    {
                        "query_id": getattr(cursor, "sfqid", None),
                        "rowcount": getattr(cursor, "rowcount", None),
                    }
                )
        finally:
            cursor.close()
        return results

    def close(self) -> None:
        self.connection.close()

