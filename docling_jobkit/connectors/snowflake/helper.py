import gzip
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, TypeVar

from docling_jobkit.connectors.snowflake.models import (
    SnowflakeChunkTarget,
    SnowflakeConnectionCoordinates,
    SnowflakeCoordinates,
    SnowflakeDocTarget,
)

_log = logging.getLogger(__name__)

_TableTarget = SnowflakeDocTarget | SnowflakeChunkTarget
_T = TypeVar("_T")

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def is_snowflake_authentication_error(exc: BaseException) -> bool:
    from snowflake.connector.errors import (
        DatabaseError,
        ForbiddenError,
        OperationalError,
        ProgrammingError,
    )

    if isinstance(exc, (OperationalError, ProgrammingError)):
        return False
    return isinstance(exc, (DatabaseError, ForbiddenError))


def is_snowflake_unavailable_error(exc: BaseException) -> bool:
    from snowflake.connector.errors import HttpError, OperationalError

    return isinstance(exc, (OperationalError, HttpError))


def _is_retryable_error(exc: BaseException) -> bool:
    if is_snowflake_unavailable_error(exc):
        return True

    # Check for rate limiting/quota errors in message
    from snowflake.connector.errors import ProgrammingError

    if isinstance(exc, ProgrammingError):
        msg = str(exc).lower()
        if any(keyword in msg for keyword in ("quota", "throttl", "rate limit")):
            return True

    return False


def with_retry(
    operation: Callable[[], _T],
    operation_name: str,
    max_retries: int = _MAX_RETRIES,
) -> _T:
    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            is_last_attempt = attempt == max_retries

            if not _is_retryable_error(exc) or is_last_attempt:
                raise

            wait = _BACKOFF_BASE * (2**attempt)
            _log.warning(
                "Transient error in %s (attempt %d/%d): %s. Retrying in %.1fs...",
                operation_name,
                attempt + 1,
                max_retries,
                exc,
                wait,
            )
            time.sleep(wait)

    # Type checker needs explicit raise (unreachable but required for type safety)
    raise RuntimeError("Retry loop exhausted")


def _load_private_key_der(pem_data: str, passphrase: str | None = None) -> bytes:
    """Convert PEM-encoded private key to DER format (Snowflake requirement).

    Snowflake expects private keys in base64-encoded DER format, not PEM.
    """
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    pem_bytes = pem_data.encode("utf-8")
    passphrase_bytes = passphrase.encode("utf-8") if passphrase else None

    private_key = serialization.load_pem_private_key(
        pem_bytes,
        password=passphrase_bytes,
        backend=default_backend(),
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection(
    coords: SnowflakeConnectionCoordinates,
) -> "Session":
    """Create a Snowpark session from connection coordinates."""
    from snowflake.snowpark import Session

    config: dict[str, Any] = {
        "account": coords.account,
        "user": coords.user,
        "warehouse": coords.warehouse,
        "database": coords.database,
        "schema": coords.db_schema,
    }
    if coords.role:
        config["role"] = coords.role

    if coords.password is not None:
        config["password"] = coords.password.get_secret_value()
    else:
        if coords.private_key is None:
            raise ValueError("Either password or private_key must be provided")

        # Snowflake/Snowpark expects DER format, not PEM
        pem_key = coords.private_key.get_secret_value()
        passphrase = (
            coords.private_key_passphrase.get_secret_value()
            if coords.private_key_passphrase
            else None
        )
        der_key = _load_private_key_der(pem_key, passphrase)
        config["private_key"] = der_key

    return Session.builder.configs(config).create()


def stage_ref(coords: SnowflakeCoordinates) -> str:
    return f"{coords.database}.{coords.db_schema}.{coords.stage}"


def list_stage_files(
    session: "Session",
    coords: SnowflakeCoordinates,
) -> Iterator[dict[str, object]]:
    path = f"@{stage_ref(coords)}"
    if coords.prefix:
        path = f"{path}/{coords.prefix.lstrip('/')}"

    sql = f"LIST '{path}'"
    if coords.pattern:
        if "'" in coords.pattern or "\\" in coords.pattern:
            raise ValueError(
                f"Snowflake PATTERN cannot contain single quotes or backslashes: {coords.pattern!r}"
            )
        sql += f" PATTERN = '{coords.pattern}'"

    # Snowpark returns DataFrame; convert rows to dicts
    df_ = session.sql(sql)
    for row in df_.collect():
        yield row.as_dict()


def relative_path_from_list_name(name: str) -> str:
    """LIST returns '<stage>/<relative path>'; drop the leading stage segment."""
    return name.split("/", 1)[1] if "/" in name else name


def download_stage_file(
    session: "Session",
    coords: SnowflakeCoordinates,
    relative_path: str,
) -> tuple[bytes, str]:
    """Download one file from stage via Snowpark's in-memory streaming.
    Snowpark provides an in-memory file streaming API (session.file.get_stream)
    snowflake-connector-python did not provide in-memory file streaming

    Snowflake's PUT defaults to AUTO_COMPRESS=TRUE and adds '.gz' suffix at upload
    time, so that suffix reliably indicates the file needs decompressing here.
    We use decompress=False and manually handle decompression based on filename.
    """
    stage_path = f"@{stage_ref(coords)}/{relative_path}"

    # Snowpark streams file directly into memory - no temp files needed
    # Use decompress=False to get raw bytes, then manually decompress if .gz
    file_stream = session.file.get_stream(stage_path, decompress=False)
    try:
        data = file_stream.read()
    finally:
        file_stream.close()

    # Manually decompress if filename indicates gzip compression
    display_name = Path(relative_path).name
    if display_name.endswith(".gz"):
        data = gzip.decompress(data)
        display_name = display_name[:-3]  # ".gz"

    return data, display_name


def table_ref(target: _TableTarget) -> str:
    return f"{target.database}.{target.db_schema}.{target.table}"


def _to_sql_literal(value: Any) -> str:
    """Convert a Python value to a SQL literal string."""
    if isinstance(value, (dict, list)):
        # JSON types: serialize and wrap in single quotes, escaping internal quotes
        json_str = json.dumps(value)
        escaped = json_str.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, str):
        # String: escape single quotes and wrap
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif value is None:
        return "NULL"
    elif isinstance(value, bool):
        # Boolean: TRUE/FALSE (before int check since bool is subclass of int)
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        # Numeric: use as-is
        return str(value)
    else:
        # Default: convert to string and escape
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


def upsert_table_rows(
    session: "Session",
    target: _TableTarget,
    id_field: str,
    rows: list[dict[str, Any]],
) -> None:
    """Upsert many rows in a single multi-row MERGE, keyed on id_field."""
    if not rows:
        return

    columns = list(rows[0].keys())
    for row in rows:
        if set(row.keys()) != set(columns):
            raise ValueError(
                f"All rows in a batch must have the same columns. "
                f"Expected {set(columns)}, got {set(row.keys())}"
            )
    if id_field not in columns:
        raise ValueError(f"Row is missing id field {id_field!r}; cannot upsert.")

    # Build VALUES clause with SQL-formatted literals
    values_rows = []
    for row in rows:
        formatted_values = [_to_sql_literal(row[c]) for c in columns]
        values_rows.append(f"({', '.join(formatted_values)})")
    values_clause = ", ".join(values_rows)

    update_list = ", ".join(f"t.{c} = s.{c}" for c in columns if c != id_field)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{c}" for c in columns)

    # No non-id columns (e.g. an empty `mappings`) -> nothing to update on a
    # match, so the MERGE only needs an insert branch.
    when_matched = f"WHEN MATCHED THEN UPDATE SET {update_list} " if update_list else ""

    sql = (
        f"MERGE INTO {table_ref(target)} AS t "
        f"USING (VALUES {values_clause}) AS s ({', '.join(columns)}) "
        f"ON t.{id_field} = s.{id_field} "
        f"{when_matched}"
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )

    # Execute without bind parameters since values are already formatted into SQL
    session.sql(sql).collect()

    _log.debug("Upserted %d rows to table %s", len(rows), table_ref(target))


def upsert_table_row(
    session: "Session",
    target: _TableTarget,
    id_field: str,
    row: dict[str, Any],
) -> None:
    """Upsert a single row"""
    upsert_table_rows(session, target, id_field, [row])


__all__ = [
    "download_stage_file",
    "get_snowflake_connection",
    "is_snowflake_authentication_error",
    "is_snowflake_unavailable_error",
    "list_stage_files",
    "relative_path_from_list_name",
    "stage_ref",
    "table_ref",
    "upsert_table_row",
    "upsert_table_rows",
    "with_retry",
]
