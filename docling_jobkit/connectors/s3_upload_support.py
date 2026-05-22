from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from docling_jobkit.config.target_config import S3PresignedConfig
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
    if hasattr(client, "upload_file"):
        client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
        )
        return

    with Path(filename).open("rb") as handle:
        client.upload_fileobj(
            Fileobj=handle,
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
    source_index: int,
    source_uri: str,
    artifact_filename: str,
) -> str:
    source_key = (
        f"{source_index:06d}-{hashlib.sha256(source_uri.encode()).hexdigest()[:12]}"
    )
    date_partition = datetime.now(timezone.utc).strftime(config.date_partition_format)

    path_parts: list[str] = []
    key_prefix = config.key_prefix.strip("/")
    if key_prefix:
        path_parts.append(key_prefix)
    if date_partition:
        path_parts.append(date_partition)

    tenant_id = task.metadata.get("tenant_id")
    if tenant_id:
        path_parts.append(_sanitize_path_component(str(tenant_id)))

    path_parts.append(_sanitize_path_component(task.task_id))
    path_parts.extend(
        [
            source_key,
            _sanitize_path_component(artifact_filename),
        ]
    )
    return "/".join(path_parts)


def _build_extra_args(
    *,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> dict[str, object]:
    extra_args: dict[str, object] = {"ContentType": content_type}
    if metadata:
        extra_args["Metadata"] = metadata
    return extra_args


def _sanitize_path_component(value: str) -> str:
    return value.replace("\\", "_").replace("/", "_")
