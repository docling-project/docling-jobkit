from typing import Annotated

from pydantic import BaseModel, Field, SecretStr, StrictStr


class S3Coordinates(BaseModel):
    endpoint: Annotated[
        StrictStr,
        Field(
            description=("S3 service endpoint, without protocol. Required."),
            examples=[
                "s3.eu-de.cloud-object-storage.appdomain.cloud",
                "s3.us-east-2.amazonaws.com ",
            ],
        ),
    ]

    verify_ssl: Annotated[
        bool,
        Field(
            description=(
                "If enabled, SSL will be used to connect to s3. "
                "Boolean. Optional, defaults to true"
            ),
        ),
    ] = True

    access_key: Annotated[
        SecretStr,
        Field(
            description=("S3 access key. Required."),
        ),
    ]

    secret_key: Annotated[
        SecretStr,
        Field(
            description=("S3 secret key. Required."),
        ),
    ]

    bucket: Annotated[
        str,
        Field(
            description=("S3 bucket name. Required."),
        ),
    ]

    key_prefix: Annotated[
        str,
        Field(
            description=(
                "Prefix for the object keys on s3. Optional, defaults to empty."
            ),
        ),
    ] = ""
