from typing import Annotated, Literal

from pydantic import BaseModel, Field, HttpUrl, SecretStr


class OllamaEmbeddingCoordinates(BaseModel):
    """Connection coordinates and embedding model settings for an Ollama embedding provider."""

    endpoint: Annotated[
        str,
        Field(
            description="Ollama API endpoint URL. Format: http://<host>:<port>",
            examples=["http://localhost:11434"],
        ),
    ]

    embedding_model: Annotated[
        str,
        Field(
            description="Embedding model name for Ollama provider.",
            examples=["nomic-embed-text"],
        ),
    ]


class OpenAIEmbeddingCoordinates(BaseModel):
    """Connection coordinates and embedding model settings for an OpenAI embedding provider."""

    api_key: Annotated[
        SecretStr,
        Field(
            description=(
                "OpenAI API key. Generate at https://platform.openai.com/api-keys. "
                "Format: sk-..."
            )
        ),
    ]

    embedding_model: Annotated[
        str,
        Field(
            description="Embedding model name for OpenAI provider.",
            examples=["text-embedding-3-small"],
        ),
    ]


class WatsonXEmbeddingCoordinates(BaseModel):
    """Connection coordinates and embedding model settings for a WatsonX embedding provider."""

    api_key: Annotated[
        SecretStr,
        Field(
            description=(
                "IBM Cloud API key for WatsonX. "
                "Generate at https://cloud.ibm.com/iam/apikeys."
            ),
        ),
    ]

    endpoint: Annotated[
        str,
        Field(
            description="WatsonX.ai endpoint URL.",
            examples=["https://us-south.ml.cloud.ibm.com"],
        ),
    ]

    project_id: Annotated[
        str,
        Field(
            description=(
                "WatsonX project ID. Go to dataplatform.cloud.ibm.com -> "
                "create or open a project -> click on manage tab"
            ),
        ),
    ]

    embedding_model: Annotated[
        str,
        Field(
            description="Embedding model name for WatsonX provider.",
            examples=["ibm/slate-30m-english-rtrvr"],
        ),
    ]


class EmbeddingConfig(BaseModel):
    """Embedding provider selection and per-provider configuration."""

    embedding_provider: Annotated[
        Literal["ollama", "openai", "watsonx"],
        Field(description="Embedding provider to use."),
    ]

    ollama: Annotated[
        OllamaEmbeddingCoordinates,
        Field(description="Ollama provider configuration."),
    ]

    openai: Annotated[
        OpenAIEmbeddingCoordinates,
        Field(description="OpenAI provider configuration."),
    ]

    watsonx: Annotated[
        WatsonXEmbeddingCoordinates,
        Field(description="WatsonX provider configuration."),
    ]


class AstraDBCoordinates(BaseModel):
    """Connection coordinates and collection settings for an AstraDB target."""

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

    embedding_config: Annotated[
        EmbeddingConfig,
        Field(
            description="Embedding provider selection and configuration.",
        ),
    ]
