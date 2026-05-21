from docling.datamodel.service.targets import PresignedUrlTarget, S3Target

from docling_jobkit.config.target_config import TargetConfig
from docling_jobkit.connectors.local_path_target_processor import (
    LocalPathTargetProcessor,
)
from docling_jobkit.connectors.s3_presigned_target_processor import (
    S3PresignedTargetProcessor,
)
from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_targets import (
    GoogleDriveTarget,
    LocalPathTarget,
    TaskTarget,
)


def get_target_processor(
    target: TaskTarget,
    task: Task | None = None,
    target_config: TargetConfig | None = None,
) -> BaseTargetProcessor:
    if isinstance(target, S3Target):
        return S3TargetProcessor(target)
    if isinstance(target, PresignedUrlTarget):
        if target_config is None or target_config.s3_presigned is None:
            raise ValueError(
                "PresignedUrlTarget requires TargetConfig.s3_presigned in orchestrator config"
            )
        if task is None:
            raise ValueError("PresignedUrlTarget requires the current task context")
        return S3PresignedTargetProcessor(target_config.s3_presigned, task)
    if isinstance(target, GoogleDriveTarget):
        from docling_jobkit.connectors.google_drive_target_processor import (
            GoogleDriveTargetProcessor,
        )

        return GoogleDriveTargetProcessor(target)
    if isinstance(target, LocalPathTarget):
        return LocalPathTargetProcessor(target)

    raise RuntimeError(f"No target processor for this target. {type(target)=}")
