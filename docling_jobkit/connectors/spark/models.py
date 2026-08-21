from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, SecretStr, model_validator

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings

# Auth Coordinates


class TokenAuth(BaseModel):
    """Bearer-token auth for generic Spark Connect endpoint"""

    kind: Literal["token"] = "token"

    token: Annotated[
        SecretStr, Field(description="Bearer token for the Spark Connect gRPC endpoint")
    ]


class DatabricksClassicAuth(BaseModel):
    """Databricks CLASSIC (all-purpose/job) cluster over Spark Connect."""

    kind: Literal["databricks_classic"] = "databricks_classic"

    token: Annotated[
        SecretStr,
        Field(
            description=(
                "Databricks personal access token (PAT)"
                "To get this, click on your profile on top right -> settings -> Developer"
                "-> Access tokens -> manage -> generate new token"
            )
        ),
    ]

    cluster_id: Annotated[
        str, Field(description="Databricks cluster id to attach the session to.")
    ]


class DatabricksServerlessAuth(BaseModel):
    """Databricks SERVERLESS SQL warehouse over databricks-sql-connector (DBAPI)."""

    kind: Literal["databricks_serverless"] = "databricks_serverless"

    token: Annotated[
        SecretStr,
        Field(
            description=(
                "Databricks personal access token (PAT)"
                "To get this, click on your profile on top right -> settings -> Developer"
                "-> Access tokens -> manage -> generate new token"
            )
        ),
    ]

    http_path: Annotated[
        str,
        Field(
            description="Serverless SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/<id> "
            "(SQL Warehouse -> Connection details).",
            examples=["/sql/1.0/warehouses/abc123"],
        ),
    ]


SparkAuth = Annotated[
    Union[TokenAuth, DatabricksClassicAuth, DatabricksServerlessAuth],
    Field(discriminator="kind"),
]

# Connector Coordinates


class SparkConnection(BaseModel):
    """Shared Spark Connect connection settings for source and targets."""

    host: Annotated[
        str,
        Field(
            description="Spark Connect gRPC host (or Databricks workspace host)",
            examples=["localhost", "https://adb-<id>.azuredatabricks.net"],
        ),
    ]

    port: Annotated[
        int, Field(description="Spark Connect port.", examples=[443, 15002])
    ]

    user_id: Annotated[
        Optional[str],
        Field(
            description="Spark Connect session user id for multi-tenant clusters; "
            "available even without auth."
        ),
    ] = None

    auth: Annotated[
        Optional[SparkAuth],
        Field(
            description="Auth for the Spark Connect endpoint. Omit for local "
            "dev (sc://host:port, no token)."
        ),
    ] = None


class SparkSourceCoordinates(SparkConnection):
    table: Annotated[
        Optional[str],
        Field(
            description="Fully-qualified source table to pull files from",
            examples=["catalog.schema.table"],
        ),
    ] = None

    query: Annotated[
        Optional[str],
        Field(
            description=(
                "Full SELECT query wrapped as a subquery. Mutually exclusive with `table`. "
                "The query must project the columns named by content_column/url_column/"
                "filename_column/id_column. Enables anti-joins against the target for "
                "incremental reads."
            )
        ),
    ] = None

    content_column: Annotated[
        Optional[str],
        Field(description="Name of the column holding the raw document bytes"),
    ] = None

    url_column: Annotated[
        Optional[str],
        Field(
            description="Alternative to content_column. Https url for Databricks File API"
        ),
    ] = None

    id_column: Annotated[
        Optional[str],
        Field(
            description="Required when using url_column. No content to hash, needs explicit ID"
        ),
    ] = None

    filename_column: Annotated[
        Optional[str],
        Field(
            description="Column holding the document file name (for format "
            "detection and name-based filtering)."
        ),
    ] = None

    max_num_elements: Annotated[
        Optional[int], Field(description="Optional cap on documents processed.")
    ] = None

    partition_column: Annotated[
        Optional[str],
        Field(
            description=(
                "Only needed for distributed mode (multi-proc/ray)"
                "can be found with DESCRIBE DETAIL <table>"
            )
        ),
    ] = None

    @model_validator(mode="after")
    def validate_columns(self) -> "SparkSourceCoordinates":
        has_content = self.content_column is not None
        has_url = self.url_column is not None

        if has_content and has_url:
            raise ValueError("content_column and url_column are mutually exclusive")
        if not has_content and not has_url:
            raise ValueError("Either content_column or url_column must be set")

        if has_url and self.id_column is None:
            raise ValueError("url_column requires id_column to be set")

        if has_url and self.partition_column is not None:
            raise ValueError("url_column cannot be used with partition_column")

        has_table = self.table is not None
        has_query = self.query is not None

        if has_table and has_query:
            raise ValueError("table and query are mutually exclusive")
        if not has_table and not has_query:
            raise ValueError("Either table or query must be set")

        if has_query and self.max_num_elements is not None:
            assert self.query is not None  # for type checker
            if "ORDER BY" not in self.query.upper():
                raise ValueError(
                    "When max_num_elements is set, query must include ORDER BY for "
                    "deterministic results"
                )

        return self


class SparkDocTarget(SparkConnection, FieldMappings):
    """For one delta row per converted document"""

    kind: Literal["spark_doc"] = "spark_doc"

    table: Annotated[str, Field(description="Destination Delta table to save doc to.")]

    table_format: Annotated[
        str,
        Field(
            description="Destination table storage format; 'delta' -> idempotent "
            "MERGE upsert, non-delta -> append (at-least-once)."
        ),
    ] = "delta"

    doc_id_field: Annotated[
        str,
        Field(description="Column used as the MERGE upsert key and document id."),
    ] = "doc_id"

    flush_batch_size: Annotated[
        int, Field(description="Rows buffered before a batch write.", gt=0)
    ] = 100


class SparkChunkTarget(SparkConnection, FieldMappings, ChunkFieldSlots):
    """One delta row per RAG chunk"""

    kind: Literal["spark_chunks"] = "spark_chunks"

    table: Annotated[
        str,
        Field(
            description="Destination Delta table to save chunks to",
            examples=["main.docling.doc_chunks"],
        ),
    ]

    table_format: Annotated[
        str,
        Field(
            description="Destination table storage format; 'delta' -> idempotent "
            "MERGE upsert, non-delta -> append (at-least-once)."
        ),
    ] = "delta"

    chunk_id_field: Annotated[
        str,
        Field(
            description="Column used as the MERGE upsert key, holding the "
            "content-addressed chunk id."
        ),
    ] = "chunk_id"

    flush_batch_size: Annotated[
        int, Field(description="Rows buffered before a batch write.", gt=0)
    ] = 100


class TaskSparkSource(SparkSourceCoordinates):
    kind: Literal["spark"] = "spark"


__all__ = [
    "DatabricksClassicAuth",
    "DatabricksServerlessAuth",
    "SparkAuth",
    "SparkChunkTarget",
    "SparkConnection",
    "SparkDocTarget",
    "SparkSourceCoordinates",
    "TaskSparkSource",
    "TokenAuth",
]
