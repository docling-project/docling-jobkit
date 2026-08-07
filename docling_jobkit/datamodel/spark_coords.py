from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, SecretStr

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings

# Auth Coordinates


class SparkAuthBase(BaseModel):
    """Fields shared by all authenticated Spark Connect variants"""

    user_id: Annotated[
        Optional[str],
        Field(description="Optional Spark Connect user id for session management"),
    ] = None


class TokenAuth(SparkAuthBase):
    """Bearer-token auth for generic Spark Connect endpoint"""

    kind: Literal["token"] = "token"

    token: Annotated[
        SecretStr, Field(description="Bearer token for the Spark Connect gRPC endpoint")
    ]


class DatabricksAuth(SparkAuthBase):
    """Databricks personal-access-token auth (PAT)"""

    kind: Literal["databricks"] = "databricks"

    token: Annotated[
        SecretStr, Field(description="Databricks personal access token (PAT)")
    ]

    cluster_id: Annotated[
        str, Field(description="Databricks cluster id to attach the session to.")
    ]


SparkAuth = Annotated[
    Union[TokenAuth, DatabricksAuth],
    Field(discriminator="kind"),
]

# Connector Coordinates


class SparkConnection(BaseModel):
    """Shared Spark Connect connection settings for source and targets."""

    host: Annotated[
        str, Field(description="Spark Connect gRPC host (or Databricks workspace host)")
    ]

    port: Annotated[
        int, Field(description="Spark Connect port.", examples=[443, 15002])
    ]

    auth: Annotated[
        Optional[SparkAuth],
        Field(
            description="Auth for the Spark Connect endpoint. Omit for local "
            "dev (sc://host:port, no token)."
        ),
    ] = None


class SparkSourceCoordinates(SparkConnection):
    table: Annotated[
        str,
        Field(
            description="Fully-qualified source table to pull files from",
            examples=["catalog.schema.table"],
        ),
    ]

    content_column: Annotated[
        str, Field(description="name of the column holding the raw document bytes")
    ]

    # NOTE: maybe make this required?
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


class SparkDocTarget(SparkConnection, FieldMappings):
    """For one delta row per converted document"""

    kind: Literal["spark_doc"] = "spark_doc"

    table: Annotated[str, Field(description="Destination Delta table to save doc to.")]

    doc_id_field: Annotated[
        str,
        Field(description="Column used as the MERGE upsert key and document id."),
    ] = "doc_id"

    flush_batch_size: Annotated[
        int, Field(description="Rows buffered before a batch write.", gt=0)
    ] = 100


class SparkChunkTarget(SparkConnection, ChunkFieldSlots):
    """One delta row per RAG chunk"""

    kind: Literal["spark_chunks"] = "spark_chunks"

    table: Annotated[
        str,
        Field(
            description="Destination Delta table to save chunks to",
            examples=["main.docling.doc_chunks"],
        ),
    ]

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
    "DatabricksAuth",
    "SparkAuth",
    "SparkChunkTarget",
    "SparkConnection",
    "SparkDocTarget",
    "SparkSourceCoordinates",
    "TaskSparkSource",
    "TokenAuth",
]
