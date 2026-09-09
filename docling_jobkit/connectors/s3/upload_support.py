from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import build_task_scoped_key
from docling_jobkit.datamodel.task import Task


def upload_s3_file(
    client,
    *,
    bucket: str,
    key: str,
    filename: str | Path,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> None:
    extra_args = _build_extra_args(
        content_type=content_type,
        metadata=metadata,
    )
    client.upload_file(
        Filename=filename,
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args,
    )


def upload_s3_object(
    client,
    *,
    bucket: str,
    key: str,
    obj: str | bytes | BinaryIO,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> None:
    if isinstance(obj, (bytes, bytearray)):
        body: BinaryIO = BytesIO(obj)
    elif isinstance(obj, str):
        body = BytesIO(obj.encode())
    else:
        body = obj

    client.upload_fileobj(
        Fileobj=body,
        Bucket=bucket,
        Key=key,
        ExtraArgs=_build_extra_args(
            content_type=content_type,
            metadata=metadata,
        ),
    )


def build_task_scoped_s3_key(
    config: S3PresignedConfig,
    task: Task,
    *,
    source_uri: str,
    artifact_filename: str,
) -> str:
    # PresignedUrlTarget writes into operator-managed storage, so the full key
    # includes the managed prefix/tenant/date/task structure before the per-source
    # hash. The storage prefix is injected by docling-serve onto
    # config.s3_coords.key_prefix when the orchestrator is constructed.
    return build_task_scoped_key(
        key_prefix=config.s3_coords.key_prefix,
        date_partition_format=config.date_partition_format,
        task=task,
        source_uri=source_uri,
        artifact_filename=artifact_filename,
    )


def _build_extra_args(
    *,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> dict[str, object]:
    extra_args: dict[str, object] = {"ContentType": content_type}
    if metadata:
        extra_args["Metadata"] = metadata
    return extra_args
