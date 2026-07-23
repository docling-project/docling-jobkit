from pydantic import BaseModel, Field

from docling.datamodel.service.sources import S3Coordinates


class S3PresignedConfig(BaseModel):
    """Server-managed presigned target policy injected by docling-serve.

    ``docling-serve`` builds this internal config from its artifact storage settings
    before constructing an orchestrator. The storage prefix lives on
    ``s3_coords.key_prefix`` so jobkit does not carry a redundant top-level field.
    """

    s3_coords: S3Coordinates
    date_partition_format: str = "%Y%m%d"
    url_expiration: int = Field(default=3600, ge=60, le=604800)
