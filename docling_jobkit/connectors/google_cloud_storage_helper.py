from datetime import datetime

from google.cloud import storage
from pydantic import BaseModel, SecretStr

from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


class GoogleCloudStorageFileIdentifier(BaseModel):
    name: str
    size: int
    last_modified: datetime | None = None


def get_client(coords: GoogleCloudStorageCoordinates) -> storage.Client:
    if coords.service_account_key:
        info = {
            k: v.get_secret_value() if isinstance(v, SecretStr) else v
            for k, v in coords.service_account_key.model_dump().items()
        }
        return storage.Client.from_service_account_info(info, project=coords.project)

    # No key provided: fall back to Application Default Credentials /
    # Workload Identity (e.g. on GKE or Cloud Run).
    return storage.Client(project=coords.project)
