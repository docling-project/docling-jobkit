from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from docling.datamodel.service.requests import (
    FileSourceRequest,
    HttpSourceRequest,
    S3SourceRequest,
)

from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates

# Compatibility aliases for historical jobkit imports. The shared request
# models in `docling` are the source-kind boundary types for file/http/s3.
TaskFileSource = FileSourceRequest
TaskHttpSource = HttpSourceRequest
TaskS3Source = S3SourceRequest


class TaskGoogleDriveSource(GoogleDriveCoordinates):
    kind: Literal["google_drive"] = "google_drive"


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
    TaskFileSource
    | TaskHttpSource
    | TaskS3Source
    | TaskGoogleDriveSource
    | TaskLocalPathSource,
    Field(discriminator="kind"),
]

__all__ = [
    "TaskFileSource",
    "TaskGoogleDriveSource",
    "TaskHttpSource",
    "TaskLocalPathSource",
    "TaskS3Source",
    "TaskSource",
]
