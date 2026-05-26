from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.service.responses import (
    ArtifactRef,
    DocumentArtifactItem,
)
from docling.utils.profiling import ProfilingItem

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import infer_artifact_type
from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor
from docling_jobkit.connectors.s3_upload_support import (
    build_task_scoped_s3_key,
    upload_s3_file,
    upload_s3_object,
)
from docling_jobkit.datamodel.task import Task

_METADATA_FIELDS = ("tenant_id", "user_id", "project_id")


class S3PresignedTargetProcessor(S3TargetProcessor):
    def __init__(self, config: S3PresignedConfig, task: Task):
        super().__init__(config.s3_coords)
        self._config = config
        self._task = task
        self._uploaded_artifacts: dict[int, list[tuple[str, str, str]]] = {}

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
        *,
        source_index: int | None = None,
        source_uri: str | None = None,
    ) -> None:
        if source_index is None or source_uri is None:
            raise ValueError(
                "S3PresignedTargetProcessor.upload_file requires source_index and source_uri"
            )
        object_key = build_task_scoped_s3_key(
            self._config,
            self._task,
            source_index=source_index,
            source_uri=source_uri,
            artifact_filename=target_filename,
        )
        metadata = self._build_object_metadata()
        upload_s3_file(
            self._client,
            bucket=self._coords.bucket,
            key=object_key,
            filename=filename,
            content_type=content_type,
            metadata=metadata,
        )
        self._uploaded_artifacts.setdefault(source_index, []).append(
            (target_filename, content_type, object_key)
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
        if source_index is None or source_uri is None:
            raise ValueError(
                "S3PresignedTargetProcessor.upload_object requires source_index and source_uri"
            )
        object_key = build_task_scoped_s3_key(
            self._config,
            self._task,
            source_index=source_index,
            source_uri=source_uri,
            artifact_filename=target_filename,
        )
        metadata = self._build_object_metadata()
        upload_s3_object(
            self._client,
            bucket=self._coords.bucket,
            key=object_key,
            obj=obj,
            content_type=content_type,
            metadata=metadata,
        )
        self._uploaded_artifacts.setdefault(source_index, []).append(
            (target_filename, content_type, object_key)
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
        uploaded = self._uploaded_artifacts.get(source_index, [])
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self._config.url_expiration
        )
        artifacts = [
            ArtifactRef(
                artifact_type=infer_artifact_type(artifact_filename),
                mime_type=mime_type,
                uri=self._client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": self._coords.bucket, "Key": object_key},
                    ExpiresIn=self._config.url_expiration,
                ),
                url_expires_at=expires_at,
            )
            for artifact_filename, mime_type, object_key in uploaded
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

    def _build_object_metadata(self) -> dict[str, str]:
        metadata: dict[str, str] = {}
        for field_name in _METADATA_FIELDS:
            value = self._task.metadata.get(field_name)
            if value is not None:
                metadata[field_name] = str(value)
        return metadata
