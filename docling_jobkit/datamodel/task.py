import datetime
import warnings
from functools import partial
from typing import Annotated, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.datamodel.chunking import (
    ChunkingExportOptions,
    ChunkingOptionType,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.s3_coords import S3Coordinates
from docling_jobkit.datamodel.task_meta import TaskProcessingMeta, TaskStatus, TaskType
from docling_jobkit.datamodel.task_targets import InBodyTarget, TaskTarget

TaskSource = Union[HttpSource, FileSource, DocumentStream, S3Coordinates]


class Task(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_id: str
    task_type: TaskType = TaskType.CONVERT
    task_status: TaskStatus = TaskStatus.PENDING
    sources: list[TaskSource] = []
    target: TaskTarget = InBodyTarget()
    options: Annotated[
        Optional[ConvertDocumentsOptions],
        Field(
            description="Deprecated, use conversion_options instead.",
            deprecated="Use conversion_options instead.",
            exclude=True,
        ),
    ] = None
    convert_options: Optional[ConvertDocumentsOptions] = None
    chunking_options: Annotated[
        Optional[ChunkingOptionType],
        Field(discriminator="chunker"),
    ] = None
    chunking_export_options: ChunkingExportOptions = ChunkingExportOptions()
    # scratch_dir: Optional[Path] = None
    processing_meta: Optional[TaskProcessingMeta] = None
    error_message: Optional[str] = None
    created_at: datetime.datetime = Field(
        default_factory=partial(datetime.datetime.now, datetime.timezone.utc)
    )
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    last_update_at: datetime.datetime = Field(
        default_factory=partial(datetime.datetime.now, datetime.timezone.utc)
    )

    @model_validator(mode="before")
    def handle_deprecated_options(cls, values):
        # Warn users if they pass the deprecated field
        if "options" in values and "conversion_options" not in values:
            warnings.warn(
                "'options' is deprecated and will be removed in a future version. "
                "Use 'conversion_options' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            values["conversion_options"] = values["options"]
        return values

    def set_status(self, status: TaskStatus):
        now = datetime.datetime.now(datetime.timezone.utc)
        if status == TaskStatus.STARTED and self.started_at is None:
            self.started_at = now
        if (
            status in [TaskStatus.SUCCESS, TaskStatus.FAILURE]
            and self.finished_at is None
        ):
            self.finished_at = now

        self.last_update_at = now
        self.task_status = status

    def is_completed(self) -> bool:
        if self.task_status in [TaskStatus.SUCCESS, TaskStatus.FAILURE]:
            return True
        return False
