"""Built-in connector plugin for docling-jobkit.

Registered under the ``docling_jobkit`` setuptools entry-point group (see
``pyproject.toml``). The connector factory calls :func:`source_connectors` /
:func:`target_connectors` to discover the connectors shipped with the package.

Imports are deferred into the functions so that merely loading this module (which
happens for every entry-point scan) stays cheap and never pulls optional, heavy
SDKs at import time. Every connector module keeps its optional SDK imports
(azure, gcloudstorage, gdrive) inside the methods that use them, so importing the
processor classes here is safe even when those extras are not installed; the
extra is only required when a connector is actually instantiated and used.
"""


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

    return {
        "source_connectors": [
            HttpSourceProcessor,
            S3SourceProcessor,
            AzureBlobSourceProcessor,
            LocalPathSourceProcessor,
            GoogleDriveSourceProcessor,
            FileNetSourceProcessor,
            GoogleCloudStorageSourceProcessor,
        ]
    }


def target_connectors():
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
    from docling_jobkit.connectors.local_path.target_processor import (
        LocalPathTargetProcessor,
    )
    from docling_jobkit.connectors.opensearch import OpenSearchTargetProcessor
    from docling_jobkit.connectors.s3.presigned_target_processor import (
        S3PresignedTargetProcessor,
    )
    from docling_jobkit.connectors.s3.target_processor import S3TargetProcessor

    return {
        "target_connectors": [
            S3TargetProcessor,
            S3PresignedTargetProcessor,
            AzureBlobTargetProcessor,
            LocalPathTargetProcessor,
            HttpPutTargetProcessor,
            GoogleDriveTargetProcessor,
            GoogleCloudStorageTargetProcessor,
            OpenSearchTargetProcessor,
        ]
    }
