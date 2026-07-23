# Util script for testing. Upload content to FileNet for later retrieval.

import base64
import json
import sys
from pathlib import Path

import requests

# should change to env vars
BASE_URL = "xxx"
USERNAME = "xxx"
API_KEY = "xxx"
REPO_ID = "xxx"
FOLDER_PATH = "xxx"


def main():
    if len(sys.argv) < 2:
        print("Usage: python dev/filenet_upload_util.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    project_root = Path(__file__).parent.parent
    filepath = project_root / "input_documents" / filename

    if not filepath.exists():
        print(f"Error: File not found: {filepath}")
        sys.exit(1)

    print(f"Uploading {filename} to {FOLDER_PATH}...")

    # Create auth header
    encoded = base64.b64encode(f"{USERNAME}:{API_KEY}".encode()).decode()
    auth_header = f"ZenApiKey {encoded}"

    # GraphQL mutation
    mutation = f"""
    mutation ($repo: String!, $contvar: String, $filename: String) {{
      createDocument(
        repositoryIdentifier: $repo
        fileInFolderIdentifier: "{FOLDER_PATH}"
        documentProperties: {{
          name: $filename
          content: $contvar
        }}
        checkinAction: {{}}
      ) {{
        id
        name
      }}
    }}
    """

    graphql_url = f"{BASE_URL.rstrip('/')}/graphql"

    # Prepare multipart request
    with open(filepath, "rb") as f:
        files = {
            "graphql": (
                None,
                json.dumps(
                    {
                        "query": mutation,
                        "variables": {
                            "repo": REPO_ID,
                            "contvar": None,
                            "filename": filename,
                        },
                    }
                ),
                "application/json",
            ),
            "contvar": (filename, f, "application/octet-stream"),
        }

        response = requests.post(
            graphql_url,
            headers={"Authorization": auth_header},
            files=files,
        )

    response.raise_for_status()
    payload = response.json()

    if "errors" in payload:
        print("Upload failed:")
        print(json.dumps(payload["errors"], indent=2))
        sys.exit(1)

    result = payload["data"]["createDocument"]
    print("Upload successful!")
    print(f"Document ID: {result['id']}")
    print(f"Document Name: {result['name']}")


if __name__ == "__main__":
    main()
