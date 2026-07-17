import logging

from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.storage.blob import BlobServiceClient, ContainerClient

from docling.datamodel.service.sources import AzureBlobCoordinates

# Suppress verbose Azure SDK HTTP logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)


def is_azure_blob_authentication_error(exc: BaseException) -> bool:
    return isinstance(exc, ClientAuthenticationError) or (
        isinstance(exc, HttpResponseError)
        and getattr(exc, "status_code", None) in {401, 403}
    )


def get_azure_blob_connection(
    coords: AzureBlobCoordinates,
) -> tuple[BlobServiceClient, ContainerClient]:
    service_client = BlobServiceClient.from_connection_string(coords.connection_string)
    container_client = service_client.get_container_client(coords.container)
    return service_client, container_client
