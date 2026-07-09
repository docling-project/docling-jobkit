from datetime import datetime

from google.cloud import storage
from pydantic import BaseModel

from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


class GoogleCloudStorageFileIdentifier(BaseModel):
    # TODO: need to investigate if theres other metadata
    name: str
    size: int | None = None
    last_modified: datetime | None = None


def get_client(coords: GoogleCloudStorageCoordinates) -> storage.Client:
    if not coords.service_account_key_path:
        raise ValueError("service_account_key_path is required for connection")

    return storage.Client.from_service_account_json(
        coords.service_account_key_path, project=coords.project
    )
