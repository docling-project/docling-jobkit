from typing import Annotated, Literal, Optional

from pydantic import Field, HttpUrl, SecretStr

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings


class AstraDBChunkTarget(FieldMappings, ChunkFieldSlots):
    """AstraDB target for chunk-level vector storage with server-side vectorization.

    Uses AstraDB's built-in vectorize feature to automatically embed chunk text
    on the server side. Good as a default offering.
    """

    kind: Literal["astradb_chunks"] = "astradb_chunks"

    # Connection settings
    api_endpoint: Annotated[
        HttpUrl,
        Field(
            description=(
                "AstraDB API endpoint URL. Available in the AstraDB console under "
                "'Connect'. Format: https://<uuid>-<region>.apps.astra.datastax.com"
            ),
            examples=["https://abc123-us-east1.apps.astra.datastax.com"],
        ),
    ]

    token: Annotated[
        SecretStr,
        Field(
            description=(
                "AstraDB application token. Generate via AstraDB console → "
                "Settings → 'Generate Token'. Format: AstraCS:…"
            ),
        ),
    ]

    keyspace: Annotated[
        str,
        Field(
            default="default_keyspace",
            description="AstraDB keyspace (namespace) to use.",
            examples=["default_keyspace", "docling"],
        ),
    ] = "default_keyspace"

    collection_name: Annotated[
        str,
        Field(
            description="Name of the AstraDB collection to write chunks into.",
            examples=["docling_chunks"],
        ),
    ]

    # Server-side vectorization configuration
    vectorize_provider: Annotated[
        str,
        Field(
            default="openai",
            description=(
                "AstraDB vectorize provider for server-side embeddings. "
                "Supported: 'openai', 'huggingface', 'nvidia', 'voyageai', etc."
            ),
            examples=["openai", "huggingface", "nvidia"],
        ),
    ] = "openai"

    vectorize_model: Annotated[
        str,
        Field(
            default="text-embedding-3-small",
            description=(
                "Model name for server-side vectorization. Must be supported by "
                "the chosen provider. Examples: 'text-embedding-3-small' (OpenAI), "
                "'sentence-transformers/all-MiniLM-L6-v2' (HuggingFace)."
            ),
            examples=[
                "text-embedding-3-small",
                "text-embedding-3-large",
                "sentence-transformers/all-MiniLM-L6-v2",
            ],
        ),
    ] = "text-embedding-3-small"

    vectorize_authentication: Annotated[
        Optional[dict[str, str]],
        Field(
            default=None,
            description=(
                "Optional authentication parameters for the vectorize provider. "
                "For OpenAI: {'providerKey': 'OPENAI_API_KEY'}. "
                "For HuggingFace: {'providerKey': 'HUGGINGFACE_API_KEY'}. "
                "If omitted, AstraDB uses its default credentials."
            ),
            examples=[
                {"providerKey": "OPENAI_API_KEY"},
                {"providerKey": "HUGGINGFACE_API_KEY"},
            ],
        ),
    ] = None

    # Override defaults for AstraDB
    coerce_large_ints_to_str: bool = Field(
        default=True,
        description=(
            "AstraDB uses 64-bit signed longs for numeric fields. "
            "Enable to stringify integers that exceed this range "
            "(e.g., DoclingDocument.origin.binary_hash)."
        ),
    )


__all__ = ["AstraDBChunkTarget"]
