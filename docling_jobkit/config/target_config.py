from pydantic import BaseModel, ConfigDict, Field, model_validator

from docling.datamodel.service.sources import AzureBlobCoordinates

from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3PresignedConfig(BaseModel):
    """Server-managed presigned target policy injected by docling-serve.

    ``docling-serve`` builds this internal config from its artifact storage settings
    before constructing an orchestrator. The storage prefix lives on
    ``s3_coords.key_prefix`` so jobkit does not carry a redundant top-level field.
    """

    s3_coords: S3Coordinates
    date_partition_format: str = "%Y%m%d"
    url_expiration: int = Field(default=3600, ge=60, le=604800)


class AzurePresignedConfig(BaseModel):
    """Server-managed Azure Blob target policy injected by docling-serve."""

    model_config = ConfigDict(hide_input_in_errors=True)

    azure_coords: AzureBlobCoordinates
    date_partition_format: str = "%Y%m%d"
    url_expiration: int = Field(default=3600, ge=60, le=604800)

    @model_validator(mode="after")
    def validate_account_key_connection_string(self) -> "AzurePresignedConfig":
        values = _parse_connection_string(self.azure_coords.connection_string)
        account_name = values.get("accountname")
        if not account_name:
            raise ValueError(
                "Azure managed artifact storage connection string must include "
                "AccountName"
            )
        if not values.get("accountkey"):
            raise ValueError(
                "Azure managed artifact storage connection string must include "
                "AccountKey"
            )
        if account_name != self.azure_coords.account_name:
            raise ValueError(
                "Azure managed artifact storage account_name must match the "
                "connection string AccountName"
            )
        return self

    def get_account_key(self) -> str:
        return _parse_connection_string(self.azure_coords.connection_string)[
            "accountkey"
        ]


PresignedConfig = S3PresignedConfig | AzurePresignedConfig


def _parse_connection_string(connection_string: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment.strip():
            continue
        key, separator, value = segment.partition("=")
        if not separator or not key.strip():
            raise ValueError(
                "Azure managed artifact storage connection string must contain "
                "semicolon-separated key=value entries"
            )
        values[key.strip().casefold()] = value.strip()
    return values
