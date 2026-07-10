import base64
import json
import logging
from io import BytesIO
from typing import Iterator

import requests

from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates

_log = logging.getLogger(__name__)


def get_filenet_auth_header(username: str, api_key: str) -> str:
    """Create ZenApiKey authorization header."""
    encoded = base64.b64encode(f"{username}:{api_key}".encode()).decode()
    return f"ZenApiKey {encoded}"


def _execute_graphql_query(
    graphql_url: str,
    auth_header: str,
    query: str,
    variables: dict | None = None,
) -> dict:
    """Execute a GraphQL query against FileNet API."""
    response = requests.post(
        graphql_url,
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "variables": variables or {},
        },
        timeout=30,
    )

    response.raise_for_status()
    payload = response.json()

    if "errors" in payload:
        error_msg = json.dumps(payload["errors"], indent=2)
        _log.error("GraphQL query failed: %s", error_msg)
        raise RuntimeError(f"GraphQL query failed: {error_msg}")

    return payload.get("data", {})


def list_repository_documents(
    coords: FileNetCoordinates,
    auth_header: str,
    page_size: int = 1000,
) -> Iterator[dict]:
    """List all documents in a FileNet repository."""
    query = """
    query ($repo: String!, $pageSize: Int) {
      documents(
        repositoryIdentifier: $repo
        pageSize: $pageSize
      ) {
        documents {
          id
          name
        }
      }
    }
    """

    graphql_url = f"{coords.base_url.rstrip('/')}/graphql"

    data = _execute_graphql_query(
        graphql_url,
        auth_header,
        query,
        {
            "repo": coords.repository_id,
            "pageSize": page_size,
        },
    )

    documents = data.get("documents", {}).get("documents", [])
    _log.info(
        "Listed %d documents from repository %s",
        len(documents),
        coords.repository_id,
    )

    for doc in documents:
        yield doc


def list_folder_documents(
    coords: FileNetCoordinates,
    auth_header: str,
    folder_id: str,
) -> Iterator[dict]:
    """List all documents in a specific FileNet folder."""
    query = """
    query ($repo: String!, $folder: String!) {
      folder(
        repositoryIdentifier: $repo
        identifier: $folder
      ) {
        name
        containedDocuments {
          documents {
            id
            name
          }
        }
      }
    }
    """

    graphql_url = f"{coords.base_url.rstrip('/')}/graphql"

    data = _execute_graphql_query(
        graphql_url,
        auth_header,
        query,
        {
            "repo": coords.repository_id,
            "folder": folder_id,
        },
    )

    folder_data = data.get("folder", {})
    documents = folder_data.get("containedDocuments", {}).get("documents", [])

    _log.info(
        "Listed %d documents from folder %s in repository %s",
        len(documents),
        folder_id,
        coords.repository_id,
    )

    for doc in documents:
        yield doc


def get_document_metadata(
    coords: FileNetCoordinates,
    auth_header: str,
    document_id: str,
) -> dict:
    """Fetch metadata for a specific FileNet document."""
    query = """
    query ($repo: String!, $doc: String!) {
      document(
        repositoryIdentifier: $repo
        identifier: $doc
      ) {
        id
        name
        mimeType
        contentSize
        contentElements {
          contentType
          elementSequenceNumber
          ... on ContentTransfer {
            contentSize
            retrievalName
            downloadUrl
          }
        }
      }
    }
    """

    graphql_url = f"{coords.base_url.rstrip('/')}/graphql"

    data = _execute_graphql_query(
        graphql_url,
        auth_header,
        query,
        {
            "repo": coords.repository_id,
            "doc": document_id,
        },
    )

    doc = data.get("document", {})

    if not doc.get("contentElements"):
        raise RuntimeError(f"Document {document_id} has no content elements")

    content_element = doc["contentElements"][0]

    return {
        "id": doc["id"],
        "name": doc["name"],
        "mimeType": doc.get("mimeType"),
        "contentSize": int(doc.get("contentSize", 0)),
        "downloadUrl": content_element["downloadUrl"],
    }


def download_document(
    base_url: str,
    auth_header: str,
    download_url: str,
) -> BytesIO:
    """Download document content from FileNet.

    Args:
        base_url: Base URL (e.g., 'https://host/content-services-graphql')
        auth_header: Authorization header
        download_url: Relative download URL from metadata (e.g., '/content?...')
    """
    clean_url = download_url.replace("&amp;", "&")
    full_url = f"{base_url.rstrip('/')}{clean_url}"

    _log.debug("Downloading from: %s", full_url)

    response = requests.get(
        full_url,
        headers={
            "Authorization": auth_header,
        },
        stream=True,
        timeout=300,
    )

    response.raise_for_status()

    buffer = BytesIO()
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            buffer.write(chunk)

    buffer.seek(0)
    return buffer
