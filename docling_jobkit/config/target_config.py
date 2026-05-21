from pydantic import BaseModel, Field

from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3PresignedConfig(BaseModel):
    s3_coords: S3Coordinates
    key_prefix: str = "converted/"
    date_partition_format: str = "%Y%m%d"
    include_tenant_in_path: bool = True
    include_task_id_in_path: bool = True
    attach_metadata: bool = True
    metadata_fields: list[str] = ["tenant_id", "user_id", "project_id"]
    url_expiration: int = Field(default=3600, ge=60, le=604800)


class TargetConfig(BaseModel):
    s3_presigned: S3PresignedConfig | None = None
