from typing import Annotated, Optional

from pydantic import BaseModel, Field, StrictStr


class GoogleCloudStorageCoordinates(BaseModel):
    bucket: Annotated[
        StrictStr,
        Field(
            description="GCS bucket name.",
            examples=["my-docling-bucket"],
        ),
    ]

    key_prefix: Annotated[
        str,
        Field(
            description="Object key prefix for traversal (sources) and output (target); defaults to bucket root.",
            examples=["incoming/docs/", "processed/"],
        ),
    ] = ""

    project: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description="GCP project ID. Optional (billing / ADC project).",
            examples=["my-gcp-project"],
        ),
    ] = None

    service_account_key_path: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description="Path to a service account JSON key file. Optional; omit → ADC / Workload Identity.",
            examples=["./dev/gcs/sa-key.json"],
        ),
    ] = None


__all__ = [
    "GoogleCloudStorageCoordinates",
]
