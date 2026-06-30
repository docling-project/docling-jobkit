from typing import Annotated

from pydantic import BaseModel, Field, HttpUrl, SecretStr


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
