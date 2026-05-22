from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates

from docling_jobkit.connectors.http_source_processor import HttpSourceProcessor
from docling_jobkit.connectors.local_path_source_processor import (
    LocalPathSourceProcessor,
)
from docling_jobkit.connectors.s3_source_processor import S3SourceProcessor
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.task_sources import (
    TaskGoogleDriveSource,
    TaskLocalPathSource,
)


def get_source_processor(
    source: (
        FileSource
        | HttpSource
        | S3Coordinates
        | TaskGoogleDriveSource
        | TaskLocalPathSource
    ),
) -> BaseSourceProcessor:
    if isinstance(source, (FileSource, HttpSource)):
        return HttpSourceProcessor(source)
    elif isinstance(source, S3Coordinates):
        return S3SourceProcessor(source)
    elif isinstance(source, TaskGoogleDriveSource):
        from docling_jobkit.connectors.google_drive_source_processor import (
            GoogleDriveSourceProcessor,
        )

        return GoogleDriveSourceProcessor(source)
    elif isinstance(source, TaskLocalPathSource):
        return LocalPathSourceProcessor(source)

    raise RuntimeError(f"No source processor for this source. {type(source)=}")
