from typing import Annotated, Literal

from pydantic import BaseModel, Field, SecretStr, model_validator

from docling_jobkit.datamodel.target_field_slots import FieldMappings


class SnowflakeConnectionCoordinates(BaseModel):
    """Shared connection/auth fields for every Snowflake source/target mode."""

    account: Annotated[
        str,
        Field(
            description=(
                "Snowflake account identifier, e.g. 'xy12345' or the "
                "'orgname-accountname' format."
            ),
            examples=["xy12345", "myorg-myaccount"],
        ),
    ]

    user: Annotated[
        str,
        Field(description="Snowflake username."),
    ]

    password: Annotated[
        SecretStr | None,
        Field(
            default=None,
            description=(
                "Password for username/password authentication. Provide this "
                "or a private key (not both)."
            ),
        ),
    ] = None

    private_key: Annotated[
        SecretStr | None,
        Field(
            default=None,
            description=(
                "PEM-encoded RSA private key contents, for key-pair "
                "authentication. Provide this or 'password' (not both)."
            ),
        ),
    ] = None

    private_key_passphrase: Annotated[
        SecretStr | None,
        Field(
            default=None,
            description="Passphrase for an encrypted private key, if any.",
        ),
    ] = None

    role: Annotated[
        str | None,
        Field(default=None, description="Snowflake role to use for the session."),
    ] = None

    warehouse: Annotated[
        str,
        Field(
            description=(
                "Warehouse used to run the SQL operations. "
                "Must be running or set to auto-resume."
            )
        ),
    ]

    database: Annotated[
        str,
        Field(description="Database that owns the stage/table."),
    ]

    db_schema: Annotated[
        str,
        Field(description="Schema that owns the stage/table."),
    ]

    @model_validator(mode="after")
    def _check_auth(self) -> "SnowflakeConnectionCoordinates":
        has_password = self.password is not None
        has_key = self.private_key is not None
        if not has_password and not has_key:
            raise ValueError(
                "Provide either 'password' or 'private_key' for authentication."
            )
        if has_password and has_key:
            raise ValueError(
                "Provide only one authentication method: 'password' or a private key."
            )
        return self


class SnowflakeCoordinates(SnowflakeConnectionCoordinates):
    stage: Annotated[
        str,
        Field(
            description="Name of the stage to traverse (unqualified, e.g. 'MY_STAGE').",
            examples=["MY_STAGE"],
        ),
    ]

    prefix: Annotated[
        str | None,
        Field(
            default=None,
            description="Optional path prefix within the stage to restrict listing to.",
            examples=["incoming/2026/"],
        ),
    ] = None

    pattern: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Optional regular expression passed to the stage LIST command's "
                "PATTERN clause to filter files by name."
            ),
            examples=[".*[.]pdf"],
        ),
    ] = None

    max_num_elements: Annotated[
        int | None,
        Field(
            default=None,
            description=(
                "Optional maximum number of documents to process. "
                "If omitted, all documents will be processed."
            ),
        ),
    ] = None


class TaskSnowflakeSource(SnowflakeCoordinates):
    kind: Literal["snowflake"] = "snowflake"


class SnowflakeDocTarget(SnowflakeConnectionCoordinates, FieldMappings):
    """Writes one row per document into a Snowflake table (upsert by id_field)."""

    kind: Literal["snowflake_doc"] = "snowflake_doc"

    table: Annotated[
        str,
        Field(
            description="Name of the table to write document rows into.",
            examples=["DOCUMENTS"],
        ),
    ]

    id_field: Annotated[
        str,
        Field(
            default="doc_id",
            description=(
                "Column used as the row's unique key for upsert (MERGE) semantics."
            ),
        ),
    ] = "doc_id"


__all__ = [
    "SnowflakeConnectionCoordinates",
    "SnowflakeCoordinates",
    "SnowflakeDocTarget",
    "TaskSnowflakeSource",
]
