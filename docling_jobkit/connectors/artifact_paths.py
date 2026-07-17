from datetime import datetime, timezone
from hashlib import sha256
from typing import Literal

from docling.datamodel.service.sources import S3Coordinates

from docling_jobkit.datamodel.task import Task

ArtifactType = Literal[
    "json", "html", "markdown", "text", "doctags", "doclang", "dclx", "resource_bundle"
]


def hash_path_component(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def build_s3_source_key(source: S3Coordinates) -> str:
    public_identity = "|".join(
        [
            source.endpoint.strip(),
            source.bucket.strip(),
            source.key_prefix.strip("/"),
        ]
    )
    return hash_path_component(public_identity)


def build_task_scoped_key(
    *,
    key_prefix: str,
    date_partition_format: str,
    task: Task,
    source_uri: str,
    artifact_filename: str,
) -> str:
    source_key = hash_path_component(source_uri)
    date_partition = datetime.now(timezone.utc).strftime(date_partition_format)

    path_parts: list[str] = []
    if normalized_prefix := key_prefix.strip("/"):
        path_parts.append(normalized_prefix)

    tenant_id = task.metadata.get("tenant_id") or "default"
    path_parts.append(_sanitize_path_component(str(tenant_id)))

    if date_partition:
        path_parts.append(date_partition)

    path_parts.extend(
        [
            _sanitize_path_component(task.task_id),
            source_key,
            _sanitize_path_component(artifact_filename),
        ]
    )
    return "/".join(path_parts)


def _sanitize_path_component(value: str) -> str:
    return value.replace("\\", "_").replace("/", "_")
