from datetime import datetime

from google.cloud import storage
from pydantic import BaseModel

from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


class GoogleCloudStorageFileIdentifier(BaseModel):
    name: str
    size: int
    last_modified: datetime | None = None


def get_client(coords: GoogleCloudStorageCoordinates) -> storage.Client:
    if coords.service_account_key_path:
        return storage.Client.from_service_account_json(
            coords.service_account_key_path, project=coords.project
        )

    # No key path: fall back to Application Default Credentials (Workload Identity)
    return storage.Client(project=coords.project)
