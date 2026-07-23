from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from docling.datamodel.service.targets import (
    InBodyTarget,
    PresignedUrlTarget,
    PutTarget,
    S3Target,
    ZipTarget,
)

# OpenSearch target models live in the opensearch connector package.
# Re-exported here so existing imports from task_targets keep working.
from docling_jobkit.connectors.opensearch.targets import (
    OpenSearchAuth,
    OpenSearchAWSIAMAuth,
    OpenSearchBasicAuth,
    OpenSearchChunkTarget,
    OpenSearchDocTarget,
)
from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)
from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates

# Re-exported from the leaf module so existing `from task_targets import ...` keeps working.
from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings


class AzureBlobTarget(AzureBlobCoordinates):
    kind: Literal["azure_blob"] = "azure_blob"


class GoogleDriveTarget(GoogleDriveCoordinates):
    kind: Literal["google_drive"] = "google_drive"


class GoogleCloudStorageTarget(GoogleCloudStorageCoordinates):
    kind: Literal["google_cloud_storage"] = "google_cloud_storage"


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


DocumentTarget = Annotated[
    InBodyTarget
    | ZipTarget
    | S3Target
    | AzureBlobTarget
    | OpenSearchDocTarget
    | PresignedUrlTarget
    | GoogleDriveTarget
    | GoogleCloudStorageTarget
    | PutTarget
    | LocalPathTarget,
    Field(discriminator="kind"),
]

ChunkTarget = Annotated[
    OpenSearchChunkTarget | PresignedUrlTarget | S3Target | ZipTarget | LocalPathTarget,
    Field(discriminator="kind"),
]

TaskTarget = DocumentTarget

__all__ = [
    "AzureBlobTarget",
    "ChunkFieldSlots",
    "ChunkTarget",
    "DocumentTarget",
    "FieldMappings",
    "GoogleCloudStorageTarget",
    "GoogleDriveTarget",
    "InBodyTarget",
    "LocalPathTarget",
    "OpenSearchAWSIAMAuth",
    "OpenSearchAuth",
    "OpenSearchBasicAuth",
    "OpenSearchChunkTarget",
    "OpenSearchDocTarget",
    "PresignedUrlTarget",
    "PutTarget",
    "S3Target",
    "TaskTarget",
    "ZipTarget",
]  # OpenSearch symbols re-exported from docling_jobkit.connectors.opensearch.targets
