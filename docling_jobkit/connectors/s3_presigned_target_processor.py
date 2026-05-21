from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Literal

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.service.responses import (
    ArtifactRef,
    DocumentArtifactItem,
)
from docling.utils.profiling import ProfilingItem

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.s3_helper import get_s3_connection
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task import Task


@dataclass
class _UploadedArtifact:
    artifact_filename: str
    mime_type: str
    object_key: str


ArtifactType = Literal["json", "html", "markdown", "text", "doctags", "resource_bundle"]


class S3PresignedTargetProcessor(BaseTargetProcessor):
    def __init__(self, config: S3PresignedConfig, task: Task):
        super().__init__()
        self._config = config
        self._task = task
        self._uploaded_artifacts: dict[int, list[_UploadedArtifact]] = {}

    def _initialize(self) -> None:
        self._client, self._resource = get_s3_connection(self._config.s3_coords)

    def _finalize(self) -> None:
        self._client.close()

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
        *,
        source_index: int | None = None,
        source_uri: str | None = None,
    ) -> None:
        with Path(filename).open("rb") as handle:
            self.upload_object(
                obj=handle,
                target_filename=target_filename,
                content_type=content_type,
                source_index=source_index,
                source_uri=source_uri,
            )

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
        *,
        source_index: int | None = None,
        source_uri: str | None = None,
    ) -> None:
        self._require_source_context(source_index=source_index, source_uri=source_uri)
        assert source_index is not None
        assert source_uri is not None

        object_key = self._build_object_key(
            source_index=source_index,
            source_uri=source_uri,
            artifact_filename=target_filename,
        )

        if isinstance(obj, (bytes, bytearray)):
            body: BinaryIO = BytesIO(obj)
        elif isinstance(obj, str):
            body = BytesIO(obj.encode())
        else:
            body = obj

        extra_args: dict[str, object] = {"ContentType": content_type}
        metadata = self._build_object_metadata()
        if metadata:
            extra_args["Metadata"] = metadata

        self._client.upload_fileobj(
            Fileobj=body,
            Bucket=self._config.s3_coords.bucket,
            Key=object_key,
            ExtraArgs=extra_args,
        )
        self._uploaded_artifacts.setdefault(source_index, []).append(
            _UploadedArtifact(
                artifact_filename=target_filename,
                mime_type=content_type,
                object_key=object_key,
            )
        )

    def build_document_artifact_item(
        self,
        *,
        source_index: int,
        source_uri: str,
        filename: str,
        status: ConversionStatus,
        errors: list[ErrorItem],
        timings: dict[str, ProfilingItem],
    ) -> DocumentArtifactItem:
        uploaded_artifacts = self._uploaded_artifacts.get(source_index, [])
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self._config.url_expiration
        )

        artifacts = [
            ArtifactRef(
                artifact_type=self._infer_artifact_type(item.artifact_filename),
                mime_type=item.mime_type,
                uri=self._client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={
                        "Bucket": self._config.s3_coords.bucket,
                        "Key": item.object_key,
                    },
                    ExpiresIn=self._config.url_expiration,
                ),
                url_expires_at=expires_at,
            )
            for item in uploaded_artifacts
        ]

        return DocumentArtifactItem(
            source_index=source_index,
            source_uri=source_uri,
            filename=filename,
            status=status,
            errors=errors,
            timings=timings,
            artifacts=artifacts,
        )

    def _build_object_key(
        self,
        *,
        source_index: int,
        source_uri: str,
        artifact_filename: str,
    ) -> str:
        source_key = (
            f"{source_index:06d}-{hashlib.sha256(source_uri.encode()).hexdigest()[:12]}"
        )
        date_partition = datetime.now(timezone.utc).strftime(
            self._config.date_partition_format
        )

        path_parts: list[str] = []
        key_prefix = self._config.key_prefix.strip("/")
        if key_prefix:
            path_parts.append(key_prefix)
        # Allow empty date_partition_format to omit the date segment entirely.
        if date_partition:
            path_parts.append(date_partition)

        tenant_id = self._task.metadata.get("tenant_id")
        if self._config.include_tenant_in_path and tenant_id:
            path_parts.append(self._sanitize_path_part(str(tenant_id)))

        if self._config.include_task_id_in_path:
            path_parts.append(self._sanitize_path_part(self._task.task_id))

        path_parts.extend(
            [
                source_key,
                self._sanitize_path_part(artifact_filename),
            ]
        )
        return "/".join(path_parts)

    def _build_object_metadata(self) -> dict[str, str]:
        if not self._config.attach_metadata:
            return {}

        metadata: dict[str, str] = {}
        for field_name in self._config.metadata_fields:
            value = self._task.metadata.get(field_name)
            if value is not None:
                metadata[field_name] = str(value)
        return metadata

    @staticmethod
    def _sanitize_path_part(value: str) -> str:
        return value.replace("\\", "_").replace("/", "_")

    @staticmethod
    def _infer_artifact_type(
        artifact_filename: str,
    ) -> ArtifactType:
        suffix = Path(artifact_filename).suffix.lower()
        if artifact_filename.endswith("_bundle.zip"):
            return "resource_bundle"
        if suffix == ".json":
            return "json"
        if suffix == ".html":
            return "html"
        if suffix == ".md":
            return "markdown"
        if suffix == ".txt":
            return "text"
        if suffix == ".doctags":
            return "doctags"
        raise ValueError(f"Unsupported artifact filename: {artifact_filename}")

    @staticmethod
    def _require_source_context(
        *,
        source_index: int | None,
        source_uri: str | None,
    ) -> None:
        if source_index is None or source_uri is None:
            raise ValueError(
                "S3PresignedTargetProcessor requires source_index and source_uri"
            )
