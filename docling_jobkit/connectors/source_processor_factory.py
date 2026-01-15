from docling_jobkit.connectors.google_drive_source_processor import (
    GoogleDriveSourceProcessor,
)
from docling_jobkit.connectors.http_source_processor import HttpSourceProcessor
from docling_jobkit.connectors.local_path_source_processor import (
    LocalPathSourceProcessor,
)
from docling_jobkit.connectors.s3_source_processor import S3SourceProcessor
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.task_sources import (
    TaskFileSource,
    TaskGoogleDriveSource,
    TaskHttpSource,
    TaskLocalPathSource,
    TaskS3Source,
    TaskSource,
)


def get_source_processor(source: TaskSource) -> BaseSourceProcessor:
    if isinstance(source, (TaskFileSource, TaskHttpSource)):
        return HttpSourceProcessor(source)
    elif isinstance(source, TaskS3Source):
        return S3SourceProcessor(source)
    elif isinstance(source, TaskGoogleDriveSource):
        return GoogleDriveSourceProcessor(source)
    elif isinstance(source, TaskLocalPathSource):
        return LocalPathSourceProcessor(source)

    raise RuntimeError(f"No source processor for this source. {type(source)=}")
