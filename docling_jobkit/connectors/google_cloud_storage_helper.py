from datetime import datetime

from google.api_core.exceptions import Forbidden, Unauthorized
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from google.cloud import storage
from pydantic import BaseModel, SecretStr

from docling.datamodel.service.sources import (
    GoogleCloudStorageCoordinates,
)


class GoogleCloudStorageFileIdentifier(BaseModel):
    name: str
    size: int
    last_modified: datetime | None = None


def is_google_cloud_storage_authentication_error(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (DefaultCredentialsError, Forbidden, RefreshError, Unauthorized),
    )


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
