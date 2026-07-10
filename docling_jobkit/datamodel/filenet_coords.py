from typing import Annotated

from pydantic import BaseModel, Field, SecretStr


class FileNetCoordinates(BaseModel):
    base_url: Annotated[
        str,
        Field(
            description=(
                "Base URL for the FileNet GraphQL API endpoint. Typically ends with '/content-services-graphql'."
            ),
            examples=["https://your-host/content-services-graphql"],
        ),
    ]

    username: Annotated[
        str,
        Field(description="FileNet username for authentication."),
    ]

    api_key: Annotated[
        SecretStr,
        Field(
            description=("API key for Zen authentication."),
        ),
    ]

    repository_id: Annotated[
        str,
        Field(
            description=("FileNet repository identifier."),
            examples=["OS1"],
        ),
    ]

    folder_id: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "Optional folder identifier or path within the repository."
                "Can be either a folder ID (e.g., '{FOLDER-ID-GUID}') or a folder path (e.g., '/test')."
                "If omitted, all documents in the repository will be processed."
                "If specified, only documents in this folder will be processed."
            ),
            examples=["/docling-test", "{FOLDER-ID-GUID}"],
        ),
    ] = None

    document_id: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "ID for individual document w/in repository."
                "If not None, will download just this one document. Overrides folder id."
            ),
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


__all__ = ["FileNetCoordinates"]
