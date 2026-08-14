"""Built-in connector plugin for docling-jobkit.

Registered under the ``docling_jobkit`` setuptools entry-point group (see
``pyproject.toml``). The connector factory calls :func:`source_connectors` /
:func:`target_connectors` to discover the connectors shipped with the package.

Imports are deferred into the functions so that merely loading this module (which
happens for every entry-point scan) stays cheap and never pulls optional, heavy
SDKs at import time. Every connector class exposes a :meth:`check_dependencies`
classmethod that performs a lightweight probe import of its required SDK. That
check is called here before registration so that connectors whose optional extra
is not installed are silently skipped (with an INFO-level log message) rather
than raising an error at startup.
"""

import logging

_log = logging.getLogger(__name__)


def _register_if_available(connectors: list, cls) -> None:
    """Append *cls* to *connectors* if its dependencies are present."""
    try:
        cls.check_dependencies()
        connectors.append(cls)
    except ImportError as exc:
        _log.info(
            "Connector %r skipped — optional dependency not installed (%s). "
            "Install the matching extra to enable it.",
            cls.__name__,
            exc,
        )


def source_connectors():
    from docling_jobkit.connectors.azure_blob.source_processor import (
        AzureBlobSourceProcessor,
    )
    from docling_jobkit.connectors.filenet.source_processor import (
        FileNetSourceProcessor,
    )
    from docling_jobkit.connectors.google_cloud_storage.source_processor import (
        GoogleCloudStorageSourceProcessor,
    )
    from docling_jobkit.connectors.google_drive.source_processor import (
        GoogleDriveSourceProcessor,
    )
    from docling_jobkit.connectors.http.source_processor import HttpSourceProcessor
    from docling_jobkit.connectors.local_path.source_processor import (
        LocalPathSourceProcessor,
    )
    from docling_jobkit.connectors.s3.source_processor import S3SourceProcessor
    from docling_jobkit.connectors.sharepoint.source_processor import (
        SharePointSourceProcessor,
    )

    connectors = [
        HttpSourceProcessor,
        LocalPathSourceProcessor,
        FileNetSourceProcessor,
    ]
    for cls in (
        S3SourceProcessor,
        AzureBlobSourceProcessor,
        GoogleDriveSourceProcessor,
        GoogleCloudStorageSourceProcessor,
        SharePointSourceProcessor,
    ):
        _register_if_available(connectors, cls)

    return {"source_connectors": connectors}


def target_connectors():
    from docling_jobkit.connectors.astradb.target_processor import (
        AstraDBTargetProcessor,
    )
    from docling_jobkit.connectors.azure_blob.target_processor import (
        AzureBlobTargetProcessor,
    )
    from docling_jobkit.connectors.google_cloud_storage.target_processor import (
        GoogleCloudStorageTargetProcessor,
    )
    from docling_jobkit.connectors.google_drive.target_processor import (
        GoogleDriveTargetProcessor,
    )
    from docling_jobkit.connectors.http.target_processor import (
        HttpPutTargetProcessor,
    )
    from docling_jobkit.connectors.kafka.target_processor import (
        KafkaChunkTargetProcessor,
    )
    from docling_jobkit.connectors.local_path.target_processor import (
        LocalPathTargetProcessor,
    )
    from docling_jobkit.connectors.opensearch.target_processor import (
        OpenSearchTargetProcessor,
    )
    from docling_jobkit.connectors.s3.presigned_target_processor import (
        S3PresignedTargetProcessor,
    )
    from docling_jobkit.connectors.s3.target_processor import S3TargetProcessor
    from docling_jobkit.connectors.sharepoint.target_processor import (
        SharePointTargetProcessor,
    )

    connectors = [
        LocalPathTargetProcessor,
        HttpPutTargetProcessor,
    ]
    for cls in (
        S3TargetProcessor,
        S3PresignedTargetProcessor,
        AzureBlobTargetProcessor,
        GoogleDriveTargetProcessor,
        GoogleCloudStorageTargetProcessor,
        OpenSearchTargetProcessor,
        AstraDBTargetProcessor,
        SharePointTargetProcessor,
        KafkaChunkTargetProcessor,
    ):
        _register_if_available(connectors, cls)

    return {"target_connectors": connectors}
