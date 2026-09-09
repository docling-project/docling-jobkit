from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.service.responses import (
    ConfidenceScores,
    DocumentArtifactItem,
)
from docling.datamodel.service.targets import PresignedUrlTarget
from docling.utils.profiling import ProfilingItem

from docling_jobkit.config.target_config import (
    AzurePresignedConfig,
    PresignedConfig,
)
from docling_jobkit.connectors.artifact_paths import ArtifactType
from docling_jobkit.connectors.azure_blob.presigned_target_processor import (
    AzureBlobPresignedTargetProcessor,
)
from docling_jobkit.connectors.s3.presigned_target_processor import (
    S3PresignedTargetProcessor,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.source_identity import SourceIdentity
from docling_jobkit.datamodel.task import Task


class PresignedTargetProcessor(BaseTargetProcessor):
    def __init__(
        self,
        target: PresignedUrlTarget,
        *,
        presigned_config: PresignedConfig,
        task: Task,
    ):
        super().__init__()
        self._processor = (
            AzureBlobPresignedTargetProcessor(
                target,
                azure_presigned_config=presigned_config,
                task=task,
            )
            if isinstance(presigned_config, AzurePresignedConfig)
            else S3PresignedTargetProcessor(
                target,
                s3_presigned_config=presigned_config,
                task=task,
            )
        )

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (PresignedUrlTarget,)

    @classmethod
    def result_mode(cls):
        return "presigned"

    def _initialize(self) -> None:
        self._processor.__enter__()

    def _finalize(self) -> None:
        self._processor.__exit__(None, None, None)

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        self._processor.upload_file(filename, target_filename, content_type)

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        self._processor.upload_object(obj, target_filename, content_type)

    def upload_artifact_file(
        self,
        *,
        source: SourceIdentity,
        artifact_type: ArtifactType,
        path: Path,
        target_filename: str,
        mime_type: str,
    ) -> None:
        self._processor.upload_artifact_file(
            source=source,
            artifact_type=artifact_type,
            path=path,
            target_filename=target_filename,
            mime_type=mime_type,
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
        return self._processor.build_document_artifact_item(
            source=source,
            filename=filename,
            status=status,
            errors=errors,
            timings=timings,
            confidence=confidence,
        )
