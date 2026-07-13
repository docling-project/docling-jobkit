from typing import Annotated, Optional

from pydantic import BaseModel, Field, SecretStr, StrictStr


class GoogleCloudStorageServiceAccountInfo(BaseModel):
    project_id: Annotated[
        StrictStr,
        Field(
            description="GCP project ID the service account belongs to.",
            examples=["my-gcp-project"],
        ),
    ]

    private_key_id: Annotated[
        SecretStr,
        Field(description="Key ID of the private key.", examples=["2aca9ed8..."]),
    ]

    private_key: Annotated[
        SecretStr,
        Field(
            description="RSA private key in PEM format.",
            examples=[
                "-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----\n"
            ],
        ),
    ]

    client_email: Annotated[
        SecretStr,
        Field(
            description="Service account email address.",
            examples=["my-sa@my-gcp-project.iam.gserviceaccount.com"],
        ),
    ]

    client_id: Annotated[
        SecretStr,
        Field(
            description="Numeric client ID of the service account.",
            examples=["111274397167470984881"],
        ),
    ]

    auth_uri: Annotated[
        StrictStr,
        Field(
            description="OAuth 2.0 authorization endpoint.",
            examples=["https://accounts.google.com/o/oauth2/auth"],
        ),
    ]

    token_uri: Annotated[
        StrictStr,
        Field(
            description="OAuth 2.0 token endpoint.",
            examples=["https://oauth2.googleapis.com/token"],
        ),
    ]

    auth_provider_x509_cert_url: Annotated[
        StrictStr,
        Field(
            description="X.509 certificate URL for the auth provider.",
            examples=["https://www.googleapis.com/oauth2/v1/certs"],
        ),
    ]

    client_x509_cert_url: Annotated[
        SecretStr,
        Field(
            description="X.509 certificate URL for the service account.",
            examples=["https://www.googleapis.com/robot/v1/metadata/x509/my-sa%40..."],
        ),
    ]

    universe_domain: Annotated[
        StrictStr,
        Field(
            description="Google Cloud universe domain.",
            examples=["googleapis.com"],
        ),
    ]


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

    max_num_elements: Annotated[
        Optional[int],
        Field(
            default=None,
            description=(
                "Maximum number of GCS objects to iterate for this source"
                "Optional, defaults to no limit"
            ),
            ge=1,
        ),
    ]

    project: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description="GCP project ID. Optional (billing / ADC project).",
            examples=["my-gcp-project"],
        ),
    ] = None

    service_account_key: Annotated[
        Optional[GoogleCloudStorageServiceAccountInfo],
        Field(
            default=None,
            description=(
                "Service account credentials. Optional; omit to use Application Default "
                "Credentials / Workload Identity (e.g. on GKE or Cloud Run).To create a key: "
                "GCP console -> IAM & Admin -> Service Accounts -> Keys -> Add Key -> JSON "
                "(the 'type' field is omitted as it is always 'service_account')."
            ),
        ),
    ] = None


__all__ = [
    "GoogleCloudStorageCoordinates",
    "GoogleCloudStorageServiceAccountInfo",
]
