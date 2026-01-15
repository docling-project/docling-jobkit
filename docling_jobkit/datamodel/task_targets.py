from pathlib import Path
from typing import Annotated, Literal

from pydantic import AnyHttpUrl, BaseModel, Field

from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class InBodyTarget(BaseModel):
    kind: Literal["inbody"] = "inbody"


class ZipTarget(BaseModel):
    kind: Literal["zip"] = "zip"


class S3Target(S3Coordinates):
    kind: Literal["s3"] = "s3"


class GoogleDriveTarget(GoogleDriveCoordinates):
    kind: Literal["google_drive"] = "google_drive"


class PutTarget(BaseModel):
    kind: Literal["put"] = "put"
    url: AnyHttpUrl


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
