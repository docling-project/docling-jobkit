import gzip
import json
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from docling_jobkit.connectors.snowflake.models import (
    SnowflakeConnectionCoordinates,
    SnowflakeCoordinates,
    SnowflakeDocTarget,
)

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


def is_snowflake_authentication_error(exc: BaseException) -> bool:
    """Classify SDK exceptions raised while connecting/authenticating.

    snowflake-connector-python layers its exceptions: ProgrammingError and
    OperationalError both subclass DatabaseError but signal SQL/execution and
    transient-network failures respectively, not bad credentials. A bare
    DatabaseError (or ForbiddenError) is what connect() raises for an invalid
    account/user/password/key/role combination.
    """
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
    # HttpError is a transport-level failure at connect() (e.g. a malformed
    # account identifier resolves to a 404), not a DatabaseError subclass, so
    # it needs its own check rather than falling out of the auth classifier.
    from snowflake.connector.errors import HttpError, OperationalError

    return isinstance(exc, (OperationalError, HttpError))


def _load_private_key_der(pem: str, passphrase: str | None) -> bytes:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        pem.encode("utf-8"),
        password=passphrase.encode("utf-8") if passphrase else None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection(
    coords: SnowflakeConnectionCoordinates,
) -> "SnowflakeConnection":
    import snowflake.connector

    kwargs: dict[str, object] = {
        "account": coords.account,
        "user": coords.user,
        "warehouse": coords.warehouse,
        "database": coords.database,
        "schema": coords.db_schema,
    }
    if coords.role:
        kwargs["role"] = coords.role

    if coords.password is not None:
        kwargs["password"] = coords.password.get_secret_value()
    else:
        assert coords.private_key is not None  # enforced by SnowflakeCoordinates
        kwargs["private_key"] = _load_private_key_der(
            coords.private_key.get_secret_value(),
            coords.private_key_passphrase.get_secret_value()
            if coords.private_key_passphrase
            else None,
        )

    return snowflake.connector.connect(**kwargs)


def stage_ref(coords: SnowflakeCoordinates) -> str:
    """Fully-qualified 'db.schema.stage' reference (without the leading '@')."""
    return f"{coords.database}.{coords.db_schema}.{coords.stage}"


def list_stage_files(
    connection: "SnowflakeConnection",
    coords: SnowflakeCoordinates,
) -> Iterator[dict[str, object]]:
    """Yield raw LIST result rows (name, size, md5, last_modified) as dicts."""
    from snowflake.connector import DictCursor

    path = f"@{stage_ref(coords)}"
    if coords.prefix:
        path = f"{path}/{coords.prefix.lstrip('/')}"

    sql = f"LIST '{path}'"
    if coords.pattern:
        escaped_pattern = coords.pattern.replace("'", "''")
        sql += f" PATTERN = '{escaped_pattern}'"

    with connection.cursor(DictCursor) as cur:
        cur.execute(sql)
        for row in cur:
            yield row  # type: ignore[misc]


def relative_path_from_list_name(name: str) -> str:
    """LIST returns '<stage>/<relative path>'; drop the leading stage segment."""
    return name.split("/", 1)[1] if "/" in name else name


def download_stage_file(
    connection: "SnowflakeConnection",
    coords: SnowflakeCoordinates,
    relative_path: str,
) -> tuple[bytes, str]:
    """Download one file from the stage and return (bytes, display_filename).

    GET is the only download primitive snowflake-connector-python exposes, and
    it only writes to a local directory. There is no in-memory API. This
    downloads into a temp directory, reads the single resulting file
    into memory, and removes the temp directory immediately: one file on disk
    at a time, never a whole batch. Probably not ideal.

    Snowflake's PUT defaults to AUTO_COMPRESS=TRUE and renames compressed
    files with a '.gz' suffix at upload time, so that suffix reliably (it is
    server-reported, not guessed) indicates the file needs decompressing here.
    """
    remote = f"'@{stage_ref(coords)}/{relative_path}'"

    with tempfile.TemporaryDirectory(prefix="docling-jobkit-snowflake-") as tmpdir:
        local_uri = f"{Path(tmpdir).as_uri()}/"
        with connection.cursor() as cur:
            cur.execute(f"GET {remote} {local_uri}")

        downloaded = list(Path(tmpdir).iterdir())
        if not downloaded:
            raise FileNotFoundError(f"Snowflake GET produced no file for {remote}")
        local_file = downloaded[0]
        data = local_file.read_bytes()

    display_name = local_file.name
    if display_name.endswith(".gz"):
        data = gzip.decompress(data)
        display_name = display_name[: -len(".gz")]

    return data, display_name


def table_ref(target: SnowflakeDocTarget) -> str:
    """Fully-qualified 'db.schema.table' reference."""
    return f"{target.database}.{target.db_schema}.{target.table}"


def _to_bind_value(value: Any) -> Any:
    """JSON-mapped fields arrive as parsed dict/list; the driver can only bind
    scalars, so serialize those back to a string before binding."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def upsert_document_row(
    connection: "SnowflakeConnection",
    target: SnowflakeDocTarget,
    row: dict[str, Any],
) -> None:
    """Upsert one document row via MERGE, keyed on target.id_field.

    Column names come from the deployer-configured `mappings`/`id_field`, not
    from converted document content, so they're interpolated as identifiers
    rather than bound as values -- same trust boundary as any other target
    connector's field-mapping-driven schema. Left unquoted (like stage_ref/
    table_ref) so Snowflake's own identifier folding applies: unquoted DDL
    (the common case, e.g. `create table t (doc_id varchar)`) is stored
    upper-cased, and unquoted references fold the same way on lookup, so a
    lower-case config value still matches. Quoting here would instead force
    exact-case matching against a name nothing actually normalized to.
    """
    id_field = target.id_field
    if id_field not in row:
        raise ValueError(f"Row is missing id field {id_field!r}; cannot upsert.")

    columns = list(row.keys())
    select_list = ", ".join(f"%s AS {c}" for c in columns)
    update_list = ", ".join(f"t.{c} = s.{c}" for c in columns if c != id_field)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{c}" for c in columns)

    # No non-id columns (e.g. an empty `mappings`) -> nothing to update on a
    # match, so the MERGE only needs an insert branch.
    when_matched = f"WHEN MATCHED THEN UPDATE SET {update_list} " if update_list else ""

    sql = (
        f"MERGE INTO {table_ref(target)} AS t "
        f"USING (SELECT {select_list}) AS s "
        f"ON t.{id_field} = s.{id_field} "
        f"{when_matched}"
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
    values = [_to_bind_value(row[c]) for c in columns]

    with connection.cursor() as cur:
        cur.execute(sql, values)


__all__ = [
    "download_stage_file",
    "get_snowflake_connection",
    "is_snowflake_authentication_error",
    "is_snowflake_unavailable_error",
    "list_stage_files",
    "relative_path_from_list_name",
    "stage_ref",
    "table_ref",
    "upsert_document_row",
]
