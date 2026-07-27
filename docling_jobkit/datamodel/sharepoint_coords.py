from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, SecretStr


class SharePointCoordinates(BaseModel):
    tenant: Annotated[
        str,
        Field(
            description=(
                "Microsoft Entra ID (Azure AD) tenant identifier or domain used as the "
                "authentication authority for the app-only client-credentials flow. "
                "Accepts a tenant GUID or a domain such as 'contoso.onmicrosoft.com'."
            ),
            examples=["contoso.onmicrosoft.com"],
        ),
    ]

    client_id: Annotated[
        str,
        Field(
            description=(
                "Application (client) ID of the Microsoft Entra app registration granted "
                "Microsoft Graph application permissions (e.g. Sites.Read.All, Files.Read.All)."
            ),
        ),
    ]

    client_secret: Annotated[
        SecretStr,
        Field(
            description=(
                "App-only client secret generated for the app registration under "
                "'Certificates & secrets'."
            ),
        ),
    ]

    site_url: Annotated[
        str,
        Field(
            description="URL of the SharePoint site to read from.",
            examples=["https://contoso.sharepoint.com/sites/Marketing"],
        ),
    ]

    # optional params
    document_library: Annotated[
        Optional[str],
        Field(
            default=None,
            description=(
                "Display name of the document library (SharePoint's top-level container, a "
                "Graph drive) to read from. A site can contain several libraries. "
                "If omitted, the site's default document library is used."
            ),
            examples=["Documents", "Contracts"],
        ),
    ] = None

    folder_path: Annotated[
        Optional[str],
        Field(
            default=None,
            description=(
                "Folder path within the document library to read from. Subfolders are "
                "traversed. If omitted, the library root is used."
            ),
            examples=["/Reports/2026"],
        ),
    ] = None

    file_ids: Annotated[
        Optional[list[str]],
        Field(
            default=None,
            description=(
                "IDs of individual items within the site to process. "
                "If set, overrides folder_path and document_library traversal."
            ),
        ),
    ] = None

    max_num_elements: Annotated[
        Optional[int],
        Field(
            default=None,
            description=(
                "Optional maximum number of documents to process. "
                "If omitted, all matching documents will be processed."
            ),
        ),
    ] = None


class TaskSharePointSource(SharePointCoordinates):
    kind: Literal["sharepoint"] = "sharepoint"


__all__ = ["SharePointCoordinates", "TaskSharePointSource"]
