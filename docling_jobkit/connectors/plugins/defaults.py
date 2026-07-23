"""Built-in connector plugin for docling-jobkit.

Registered under the ``docling_jobkit`` setuptools entry-point group (see
``pyproject.toml``). The connector factory calls :func:`source_connectors` /
:func:`target_connectors` to discover the connectors shipped with the package.

Imports are deferred into the functions so that merely loading this module (which
happens for every entry-point scan) stays cheap and never pulls optional, heavy
SDKs at import time. The Google Drive connectors only import their google
dependencies inside their methods, so listing the classes here is import-safe
even when the ``gdrive`` extra is not installed.
"""


def source_connectors():
    from docling_jobkit.connectors.azure_blob_source_processor import (
        AzureBlobSourceProcessor,
    )
    from docling_jobkit.connectors.google_cloud_storage_source_processor import (
        GoogleCloudStorageSourceProcessor,
    )
    from docling_jobkit.connectors.google_drive_source_processor import (
        GoogleDriveSourceProcessor,
    )
    from docling_jobkit.connectors.http_source_processor import HttpSourceProcessor
    from docling_jobkit.connectors.local_path_source_processor import (
        LocalPathSourceProcessor,
    )
    from docling_jobkit.connectors.s3_source_processor import S3SourceProcessor

    return {
        "source_connectors": [
            HttpSourceProcessor,
            S3SourceProcessor,
            AzureBlobSourceProcessor,
            LocalPathSourceProcessor,
            GoogleDriveSourceProcessor,
            GoogleCloudStorageSourceProcessor,
        ]
    }


def target_connectors():
    from docling_jobkit.connectors.azure_blob_target_processor import (
        AzureBlobTargetProcessor,
    )
    from docling_jobkit.connectors.google_cloud_storage_target_processor import (
        GoogleCloudStorageTargetProcessor,
    )
    from docling_jobkit.connectors.google_drive_target_processor import (
        GoogleDriveTargetProcessor,
    )
    from docling_jobkit.connectors.http_put_target_processor import (
        HttpPutTargetProcessor,
    )
    from docling_jobkit.connectors.local_path_target_processor import (
        LocalPathTargetProcessor,
    )
    from docling_jobkit.connectors.opensearch import OpenSearchTargetProcessor
    from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor

    return {
        "target_connectors": [
            S3TargetProcessor,
            AzureBlobTargetProcessor,
            LocalPathTargetProcessor,
            HttpPutTargetProcessor,
            GoogleDriveTargetProcessor,
            GoogleCloudStorageTargetProcessor,
            OpenSearchTargetProcessor,
        ]
    }
