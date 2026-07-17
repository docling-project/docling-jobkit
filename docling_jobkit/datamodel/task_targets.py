from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from docling.datamodel.service.targets import (
    AzureBlobTarget,
    GoogleCloudStorageTarget,
    GoogleDriveTarget,
    InBodyTarget,
    PresignedUrlTarget,
    PutTarget,
    S3Target,
    ZipTarget,
)


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
    | AzureBlobTarget
    | PresignedUrlTarget
    | GoogleDriveTarget
    | GoogleCloudStorageTarget
    | PutTarget
    | LocalPathTarget,
    Field(discriminator="kind"),
]

__all__ = [
    "AzureBlobTarget",
    "GoogleCloudStorageTarget",
    "GoogleDriveTarget",
    "InBodyTarget",
    "LocalPathTarget",
    "PresignedUrlTarget",
    "PutTarget",
    "S3Target",
    "TaskTarget",
    "ZipTarget",
]
