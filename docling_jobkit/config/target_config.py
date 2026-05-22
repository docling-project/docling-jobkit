from pydantic import BaseModel, Field

from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3PresignedConfig(BaseModel):
    s3_coords: S3Coordinates
    key_prefix: str = "converted/"
    date_partition_format: str = "%Y%m%d"
    url_expiration: int = Field(default=3600, ge=60, le=604800)
