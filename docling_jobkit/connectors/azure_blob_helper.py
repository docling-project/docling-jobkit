"""Azure Blob Storage connection management and utilities."""

import logging

from azure.storage.blob import BlobServiceClient, ContainerClient

from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates

_log = logging.getLogger(__name__)

# Suppress verbose Azure SDK HTTP logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


def get_azure_blob_connection(
    coords: AzureBlobCoordinates,
) -> tuple[BlobServiceClient, ContainerClient]:
    service_client = BlobServiceClient.from_connection_string(coords.connection_string)
    container_client = service_client.get_container_client(coords.container)
    return service_client, container_client
