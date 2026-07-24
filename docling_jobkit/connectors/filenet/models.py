from typing import Annotated, Literal

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
    document_ids: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=(
                "IDs for individual documents within the repository. "
                "Currently limited to one document. If not empty, overrides folder_id."
            ),
        ),
    ]
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


class TaskFileNetSource(FileNetCoordinates):
    kind: Literal["filenet"] = "filenet"


__all__ = ["FileNetCoordinates", "TaskFileNetSource"]
