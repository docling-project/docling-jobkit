from typing import Annotated, List, Optional

from pydantic import BaseModel, Field, HttpUrl, SecretStr, StrictStr


class GoogleDriveCredentials(BaseModel):
    client_id: Annotated[
        StrictStr,
        Field(
            description="OAuth 2.0 Client ID issued by Google. Required.",
        ),
    ]

    project_id: Annotated[
        StrictStr,
        Field(
            description="Google Cloud project ID associated with the OAuth client. Required.",
            examples=["docling-test-473014"],
        ),
    ]

    auth_uri: Annotated[
        HttpUrl,
        Field(
            description="Authorization endpoint URI. Required.",
            examples=["https://accounts.google.com/o/oauth2/auth"],
        ),
    ]

    token_uri: Annotated[
        HttpUrl,
        Field(
            description="Token endpoint URI. Required.",
            examples=["https://oauth2.googleapis.com/token"],
        ),
    ]

    auth_provider_x509_cert_url: Annotated[
        HttpUrl,
        Field(
            description="Certs URL for Google's OAuth provider. Required.",
            examples=["https://www.googleapis.com/oauth2/v1/certs"],
        ),
    ]

    client_secret: Annotated[
        SecretStr,
        Field(
            description="OAuth 2.0 client secret. Required.",
        ),
    ]

    redirect_uris: Annotated[
        List[HttpUrl],
        Field(
            description="OAuth 2.0 redirect URIs. Required.",
            examples=[["http://localhost"]],
        ),
    ]


class GoogleDriveCoordinates(BaseModel):
    path_id: Annotated[
        StrictStr,
        Field(
            description=(
                "Identifier for a file or folder in Google Drive. It can be obtained from the URL as follows:"
                "Folder: https://drive.google.com/drive/u/0/folders/1yucgL9WGgWZdM1TOuKkeghlPizuzMYb5 -> folder id is 1yucgL9WGgWZdM1TOuKkeghlPizuzMYb5"
                "File: https://docs.google.com/document/d/1bfaMQ18_i56204VaQDVeAFpqEijJTgvurupdEDiaUQw/edit -> document id is 1bfaMQ18_i56204VaQDVeAFpqEijJTgvurupdEDiaUQw."
                "Required."
            ),
            examples=[
                "11hgbUnDr-fyX4Hsi3T2q3xvYimvkOrfN",
            ],
        ),
    ]

    token_path: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description=(
                "Path to save the OAuth 2.0 access token, which is generated on the fly. One of 'token_path' or 'refresh_token' is required."
            ),
            examples=[
                "./dev/google_drive_token.json",
            ],
        ),
    ]

    refresh_token: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description=(
                "Refresh token for the OAuth 2.0 access, if already pre-generated. One of 'token_path' or 'refresh_token' is required."
            ),
        ),
    ]

    credentials_path: Annotated[
        Optional[StrictStr],
        Field(
            default=None,
            description=(
                "Path to the OAuth 2.0 Client ID credentials (available in Google Cloud console). One of 'credentials_path' or 'credentials' is required."
            ),
            examples=[
                "./dev/google_drive_credentials.json",
            ],
        ),
    ]

    credentials: Annotated[
        Optional[GoogleDriveCredentials],
        Field(
            default=None,
            description="OAuth 2.0 Client ID' credentials (available in Google Cloud console). One of 'credentials_path' or 'credentials' is required.",
        ),
    ]
