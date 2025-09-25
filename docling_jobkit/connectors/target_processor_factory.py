from docling_jobkit.connectors.google_drive_target_processor import (
    GoogleDriveTargetProcessor,
)
from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task_targets import (
    GoogleDriveTarget,
    S3Target,
    TaskTarget,
)


def get_target_processor(target: TaskTarget) -> BaseTargetProcessor:
    if isinstance(target, S3Target):
        return S3TargetProcessor(target)
    if isinstance(target, GoogleDriveTarget):
        return GoogleDriveTargetProcessor(target)

    raise RuntimeError(f"No target processor for this target. {type(target)=}")
