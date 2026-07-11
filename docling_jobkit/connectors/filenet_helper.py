import base64
import json
import logging
import time
from io import BytesIO
from typing import Callable, Iterator, TypeVar

import requests

from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates

_log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE_S = 0.5
_RETRYABLE_4XX_STATUS = {429}

T = TypeVar("T")


# I dont think we need to add jitter/randomness for the cli case but maybe for distributed version
# TODO: add logic to extract Retry-After header sent in http header by external api on 429 telling us
# after how much time the rate limit will be dropped and we are good to retry
def _with_exponential_retry(fn: Callable[[], T], operation: str) -> T:
    """helper for exponential retries on transient errors"""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            result = fn()
            if isinstance(result, requests.Response):
                result.raise_for_status()

            return result
        except (requests.Timeout, requests.ConnectionError):
            if attempt == _MAX_RETRIES:
                raise
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            # 4xx are permanent (except 429) so we bail immediately
            if (
                status is not None
                and status < 500
                and status not in _RETRYABLE_4XX_STATUS
            ):
                raise
            if attempt == _MAX_RETRIES:
                raise

        wait = _BACKOFF_BASE_S * (2**attempt)
        logging.warning(
            "FileNet: %s transient error, retry %d/%d in %.1fs",
            operation,
            attempt + 1,
            _MAX_RETRIES,
            wait,
        )
        time.sleep(wait)

    raise AssertionError("unreachable")


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
    response = _with_exponential_retry(
        lambda: requests.post(
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
        ),
        "GraphQL query",
    )

    payload = response.json()

    if "errors" in payload:
        error_msg = json.dumps(payload["errors"], indent=2)
        _log.error("GraphQL query failed: %s", error_msg)
        raise RuntimeError(f"GraphQL query failed: {error_msg}")

    return payload.get("data", {})


def _paginate_documents(
    graphql_url: str,
    auth_header: str,
    first_set: dict,  # a DocumentSet: {documents, pageInfo}
) -> Iterator[dict]:
    """Yield documents from a DocumentSet, paginating via pageInfo.token + moreDocuments."""
    page = 1
    current = first_set

    while True:
        for doc in current.get("documents", []):
            yield doc

        token = (current.get("pageInfo") or {}).get("token")
        if not token:
            return

        page += 1
        query = """
        query ($token: String!) {
            moreDocuments(token: $token) {
                documents {
                    id
                    name
                }
                pageInfo {
                    token
                }
            }
        }
        """

        data = _execute_graphql_query(graphql_url, auth_header, query, {"token": token})

        current = data.get("moreDocuments", {})


def check_connection(coords: FileNetCoordinates, auth_header: str) -> None:
    """Verify connectivity, credentials, and repository before processing (fail fast)"""
    query = """
    query ($repo: String!) {
        documents(
            repositoryIdentifier: $repo
            pageSize: 1
        ) {
            documents {
                id
            }
            pageInfo {
                totalCount
            }
        }
    }
    """

    graphql_url = f"{coords.base_url.rstrip('/')}/graphql"

    _execute_graphql_query(
        graphql_url, auth_header, query, {"repo": coords.repository_id}
    )


def list_repository_documents(
    coords: FileNetCoordinates,
    auth_header: str,
    page_size: int = 1000,
) -> Iterator[dict]:
    """List all documents in a FileNet repository, paginating across all pages."""
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
        pageInfo {
            token
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

    doc_count = 0
    try:
        for doc in _paginate_documents(
            graphql_url, auth_header, data.get("documents", {})
        ):
            doc_count += 1
            yield doc
    finally:
        _log.info(
            "Listed %d documents from repository %s",
            doc_count,
            coords.repository_id,
        )


def list_folder_documents(
    coords: FileNetCoordinates,
    auth_header: str,
    folder_id: str,
) -> Iterator[dict]:
    """List all documents in a specific FileNet folder, paginating across all pages"""
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
          pageInfo {
            token
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

    contained = data.get("folder", {}).get("containedDocuments", {})

    doc_count = 0
    try:
        for doc in _paginate_documents(graphql_url, auth_header, contained):
            doc_count += 1
            yield doc
    finally:
        _log.info(
            "Listed %d documents from folder %s in repository %s",
            doc_count,
            folder_id,
            coords.repository_id,
        )


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

    response = _with_exponential_retry(
        lambda: requests.get(
            full_url,
            headers={
                "Authorization": auth_header,
            },
            stream=True,
            timeout=300,
        ),
        "document download",
    )

    buffer = BytesIO()
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            buffer.write(chunk)

    buffer.seek(0)
    return buffer
