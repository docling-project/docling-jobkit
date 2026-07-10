# Test script for verifying graphql file download works.

import sys
from pathlib import Path

from pydantic import SecretStr

from docling_jobkit.connectors.filenet_helper import (
    download_document,
    get_document_metadata,
    get_filenet_auth_header,
)
from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates

# should change to env vars
BASE_URL = "xxx"
USERNAME = "xxx"
API_KEY = "xxx"
REPOSITORY_ID = "xxx"
DOCUMENT_ID = "xxx"

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    print(f"Base URL: {BASE_URL}")
    print(f"Repository: {REPOSITORY_ID}")
    print(f"Document ID: {DOCUMENT_ID}")
    print()

    coords = FileNetCoordinates(
        base_url=BASE_URL,
        username=USERNAME,
        api_key=SecretStr(API_KEY),
        repository_id=REPOSITORY_ID,
    )

    auth_header = get_filenet_auth_header(USERNAME, API_KEY)

    print("Fetching metadata...")
    metadata = get_document_metadata(coords, auth_header, DOCUMENT_ID)
    print(f"  Name: {metadata['name']}")
    print(f"  Size: {metadata['contentSize']:,} bytes")
    print(f"  MIME: {metadata.get('mimeType', 'N/A')}")
    print(f"  Download URL: {metadata['downloadUrl']}")
    print()

    print("Downloading document...")
    print(f"BASE URL: {BASE_URL}")
    print(f"RAW URL: {metadata['downloadUrl']}")

    clean_url = metadata["downloadUrl"].replace("&amp;", "&")
    full_url = f"{BASE_URL.rstrip('/')}{clean_url}"
    print(f"  FINAL URL: {full_url}")
    print()

    buffer = download_document(BASE_URL, auth_header, metadata["downloadUrl"])

    output_dir = project_root / "output_documents"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / metadata["name"]

    with open(output_path, "wb") as f:
        f.write(buffer.getvalue())

    print(f"Downloaded {len(buffer.getvalue()):,} bytes")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
