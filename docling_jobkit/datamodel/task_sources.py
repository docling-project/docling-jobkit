from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from docling.datamodel.service.requests import (
    FileSourceRequest,
    HttpSourceRequest,
    S3SourceRequest,
)

from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates
from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)
from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates


class TaskAzureBlobSource(AzureBlobCoordinates):
    kind: Literal["azure_blob"] = "azure_blob"


class TaskGoogleDriveSource(GoogleDriveCoordinates):
    kind: Literal["google_drive"] = "google_drive"


class TaskFileNetSource(FileNetCoordinates):
    kind: Literal["filenet"] = "filenet"


class TaskGoogleCloudStorageSource(GoogleCloudStorageCoordinates):
    kind: Literal["google_cloud_storage"] = "google_cloud_storage"


class TaskLocalPathSource(BaseModel):
    kind: Literal["local_path"] = "local_path"

    path: Annotated[
        Path,
        Field(
            description=(
                "Local filesystem path to a file or directory. "
                "For files, the single file will be processed. "
                "For directories, files will be discovered based on the pattern and recursive settings. "
                "Required."
            ),
            examples=[
                "/path/to/document.pdf",
                "/path/to/documents/",
                "./data/input/",
            ],
        ),
    ]

    pattern: Annotated[
        str,
        Field(
            description=(
                "Glob pattern for matching files within a directory. "
                "Supports standard glob syntax (e.g., '*.pdf', '**/*.docx'). "
                "Only applicable when path is a directory. "
                "Optional, defaults to '*' (all files)."
            ),
            examples=[
                "*.pdf",
                "*.{pdf,docx}",
                "**/*.pdf",
                "report_*.pdf",
            ],
        ),
    ] = "*"

    recursive: Annotated[
        bool,
        Field(
            description=(
                "If True, recursively traverse subdirectories when path is a directory. "
                "If False, only process files in the immediate directory. "
                "Optional, defaults to True."
            ),
        ),
    ] = True


TaskSource = Annotated[
    FileSourceRequest
    | HttpSourceRequest
    | S3SourceRequest
    | TaskAzureBlobSource
    | TaskGoogleDriveSource
    | TaskFileNetSource
    | TaskGoogleCloudStorageSource
    | TaskLocalPathSource,
    Field(discriminator="kind"),
]

__all__ = [
    "TaskAzureBlobSource",
    "TaskFileNetSource",
    "TaskGoogleCloudStorageSource",
    "TaskGoogleDriveSource",
    "TaskLocalPathSource",
    "TaskSource",
]
