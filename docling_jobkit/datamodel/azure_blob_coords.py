"""Azure Blob Storage coordinates for source and target configuration."""

from typing import Annotated

from pydantic import BaseModel, Field


class AzureBlobCoordinates(BaseModel):
    """Azure Blob Storage connection coordinates."""

    account_name: Annotated[
        str,
        Field(description="Azure Storage account name"),
    ]

    container: Annotated[
        str,
        Field(description="Azure Blob container name"),
    ]

    connection_string: Annotated[
        str,
        Field(description="Azure Storage connection string for authentication"),
    ]

    blob_prefix: Annotated[
        str,
        Field(default="", description="Prefix for blob names"),
    ] = ""

    max_num_elements: Annotated[
        int | None,
        Field(default=None, description="Maximum number of blobs to process"),
    ] = None


__all__ = ["AzureBlobCoordinates"]
