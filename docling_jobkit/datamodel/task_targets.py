from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from docling.datamodel.service.targets import (
    InBodyTarget,
    PutTarget,
    S3Target,
    ZipTarget,
)

from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates


class GoogleDriveTarget(GoogleDriveCoordinates):
    kind: Literal["google_drive"] = "google_drive"


class LocalPathTarget(BaseModel):
    kind: Literal["local_path"] = "local_path"

    path: Annotated[
        Path,
        Field(
            description=(
                "Local filesystem path for output. "
                "Can be a directory (outputs will be written inside) or a file path. "
                "Directories will be created if they don't exist. "
                "Required."
            ),
            examples=[
                "/path/to/output/",
                "./data/output/",
                "/path/to/output.json",
            ],
        ),
    ]


TaskTarget = Annotated[
    InBodyTarget
    | ZipTarget
    | S3Target
    | GoogleDriveTarget
    | PutTarget
    | LocalPathTarget,
    Field(discriminator="kind"),
]

__all__ = [
    "GoogleDriveTarget",
    "InBodyTarget",
    "LocalPathTarget",
    "PutTarget",
    "S3Target",
    "TaskTarget",
    "ZipTarget",
]
