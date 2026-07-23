import datetime
import warnings
from collections.abc import Mapping
from functools import partial
from typing import Annotated, Any, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SerializeAsAny,
    ValidationInfo,
    model_validator,
)

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.callbacks import CallbackSpec
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import PublicFailureInfo
from docling.datamodel.service.targets import InBodyTarget, ZipTarget
from docling.datamodel.service.tasks import TaskProcessingMeta, TaskType

from docling_jobkit.datamodel.chunking import (
    ChunkingExportOptions,
    ChunkingOptionType,
)
from docling_jobkit.datamodel.task_meta import TaskStatus


def _resolve_source(value: Any, info: ValidationInfo) -> Any:
    if isinstance(value, DocumentStream):
        return value

    from docling_jobkit.connectors.connector_factory import (
        get_source_connector_factory,
    )

    allow_external_plugins = bool(
        (info.context or {}).get("allow_external_plugins", False)
    )
    return get_source_connector_factory(allow_external_plugins).validate_config(value)


# The factory returns the exact concrete model. Pydantic's default
# revalidate_instances="never" preserves that instance under the BaseModel branch.
TaskSource = Annotated[
    DocumentStream | SerializeAsAny[BaseModel],
    BeforeValidator(_resolve_source),
]


def _resolve_target(value: Any, info: ValidationInfo) -> Any:
    if isinstance(value, (InBodyTarget, ZipTarget)):
        return value
    if isinstance(value, Mapping):
        if value.get("kind") == "inbody":
            return InBodyTarget.model_validate(value)
        if value.get("kind") == "zip":
            return ZipTarget.model_validate(value)

    from docling_jobkit.connectors.connector_factory import (
        get_target_connector_factory,
    )

    allow_external_plugins = bool(
        (info.context or {}).get("allow_external_plugins", False)
    )
    return get_target_connector_factory(allow_external_plugins).validate_config(value)


def _resolve_chunk_target(value: Any, info: ValidationInfo) -> Any:
    if value is None:
        return value
    allow_external_plugins = bool(
        (info.context or {}).get("allow_external_plugins", False)
    )
    from docling_jobkit.connectors.connector_factory import (
        get_target_connector_factory,
    )

    return get_target_connector_factory(allow_external_plugins).validate_config(value)


TaskTarget = Annotated[
    InBodyTarget | ZipTarget | SerializeAsAny[BaseModel],
    BeforeValidator(_resolve_target),
]

ChunkTarget = Annotated[
    SerializeAsAny[BaseModel],
    BeforeValidator(_resolve_chunk_target),
]


class Task(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        hide_input_in_errors=True,
    )

    task_id: str
    task_type: TaskType = TaskType.CONVERT
    task_status: TaskStatus = TaskStatus.PENDING
    sources: list[TaskSource] = []
    target: TaskTarget = InBodyTarget()
    chunk_target: Optional[ChunkTarget] = None  # type: ignore[valid-type]
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
    callbacks: list[CallbackSpec] = []
    # scratch_dir: Optional[Path] = None
    processing_meta: Optional[TaskProcessingMeta] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    failure: Optional[PublicFailureInfo] = None
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
            values["convert_options"] = values["options"]
        return values

    @model_validator(mode="after")
    def validate_chunk_target(self):
        convert_options = self.convert_options
        do_chunking = convert_options is not None and convert_options.do_chunking

        if self.chunk_target is not None and not do_chunking:
            raise ValueError("chunk_target requires convert_options.do_chunking=True")

        if self.task_type == TaskType.CHUNK and self.chunk_target is not None:
            raise ValueError("chunk_target is only supported for convert tasks")

        return self

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


def validate_task(
    payload: Mapping[str, Any], *, allow_external_plugins: bool = False
) -> Task:
    return Task.model_validate(
        payload,
        context={"allow_external_plugins": allow_external_plugins},
    )


def validate_task_json(
    payload: str | bytes, *, allow_external_plugins: bool = False
) -> Task:
    return Task.model_validate_json(
        payload,
        context={"allow_external_plugins": allow_external_plugins},
    )
