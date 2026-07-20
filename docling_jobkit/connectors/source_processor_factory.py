from docling.datamodel.service.sources import (
    AzureBlobCoordinates,
    FileSource,
    GoogleCloudStorageCoordinates,
    GoogleDriveCoordinates,
    HttpSource,
    S3Coordinates,
)

from docling_jobkit.connectors.connector_factory import get_source_connector_factory
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.task_sources import TaskLocalPathSource


def get_source_processor(
    source: (
        FileSource
        | HttpSource
        | S3Coordinates
        | AzureBlobCoordinates
        | GoogleCloudStorageCoordinates
        | GoogleDriveCoordinates
        | TaskLocalPathSource
    ),
    *,
    allow_external_plugins: bool = False,
) -> BaseSourceProcessor:
    """Instantiate the source processor for ``source`` via the connector factory.

    Thin backward-compatible wrapper: dispatch is now driven by the pluggy-based
    :class:`SourceConnectorFactory` (keyed on the config model's ``kind``), so new
    source connectors are added by registering a plugin rather than editing this
    function.
    """
    factory = get_source_connector_factory(allow_external_plugins)
    return factory.create_instance(source)
