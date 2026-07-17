from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.service.responses import (
    ArtifactRef,
    ConfidenceScores,
    DocumentArtifactItem,
)
from docling.utils.profiling import ProfilingItem

from docling_jobkit.config.target_config import AzurePresignedConfig
from docling_jobkit.connectors.artifact_paths import (
    ArtifactType,
    build_task_scoped_key,
)
from docling_jobkit.connectors.azure_blob_target_processor import (
    AzureBlobTargetProcessor,
)
from docling_jobkit.datamodel.source_identity import SourceIdentity
from docling_jobkit.datamodel.task import Task

_METADATA_FIELDS = ("tenant_id", "user_id", "project_id")


class AzureBlobPresignedTargetProcessor(AzureBlobTargetProcessor):
    def __init__(self, config: AzurePresignedConfig, task: Task):
        super().__init__(config.azure_coords)
        self._config = config
        self._task = task
        self._account_key = config.get_account_key()
        self._uploaded_artifacts: dict[int, list[tuple[ArtifactType, str, str]]] = {}

    def upload_artifact_file(
        self,
        *,
        source: SourceIdentity,
        artifact_type: ArtifactType,
        path: Path,
        target_filename: str,
        mime_type: str,
    ) -> None:
        from docling_jobkit.connectors.azure_blob_upload_support import (
            upload_azure_blob_file,
        )

        blob_name = build_task_scoped_key(
            key_prefix=self._coords.blob_prefix,
            date_partition_format=self._config.date_partition_format,
            task=self._task,
            source_uri=source.source_uri,
            artifact_filename=target_filename,
        )
        blob_client = self._container_client.get_blob_client(blob_name)
        upload_azure_blob_file(
            blob_client,
            filename=path,
            content_type=mime_type,
            metadata=self._build_object_metadata(),
        )
        self._uploaded_artifacts.setdefault(source.source_index, []).append(
            (artifact_type, mime_type, blob_name)
        )

    def build_document_artifact_item(
        self,
        *,
        source: SourceIdentity,
        filename: str,
        status: ConversionStatus,
        errors: list[ErrorItem],
        timings: dict[str, ProfilingItem],
        confidence: ConfidenceScores | None = None,
    ) -> DocumentArtifactItem:
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self._config.url_expiration
        )
        artifacts = [
            ArtifactRef(
                artifact_type=artifact_type,
                mime_type=mime_type,
                uri=self._build_sas_url(blob_name, expires_at),
                url_expires_at=expires_at,
            )
            for artifact_type, mime_type, blob_name in self._uploaded_artifacts.get(
                source.source_index, []
            )
        ]
        return DocumentArtifactItem(
            source_index=source.source_index,
            source_uri=source.source_uri,
            filename=filename,
            status=status,
            errors=errors,
            timings=timings,
            artifacts=artifacts,
            confidence=confidence,
        )

    def _build_sas_url(self, blob_name: str, expires_at: datetime) -> str:
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        blob_client = self._container_client.get_blob_client(blob_name)
        sas = generate_blob_sas(
            account_name=self._coords.account_name,
            container_name=self._coords.container,
            blob_name=blob_name,
            account_key=self._account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expires_at,
        )
        return f"{blob_client.url}?{sas}"

    def _build_object_metadata(self) -> dict[str, str]:
        metadata: dict[str, str] = {}
        for field_name in _METADATA_FIELDS:
            value = self._task.metadata.get(field_name)
            if value is not None:
                metadata[field_name] = str(value)
        return metadata
