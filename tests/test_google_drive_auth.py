from unittest.mock import MagicMock, patch

import pytest

from docling.datamodel.service.sources import (
    GoogleDriveCoordinates,
    GoogleDriveCredentials,
)

from docling_jobkit.connectors.auth_context import (
    allow_interactive_auth,
    is_interactive_auth_allowed,
)
from docling_jobkit.connectors.errors import (
    ConnectorAuthenticationError,
    SourceConnectorAuthenticationError,
)
from docling_jobkit.connectors.google_drive.source_processor import (
    GoogleDriveSourceProcessor,
)

try:
    from google.auth.exceptions import RefreshError

    from docling_jobkit.connectors.google_drive.helper import get_service
except ImportError:
    pytest.skip("Google Drive dependencies are not installed", allow_module_level=True)


def _coordinates() -> GoogleDriveCoordinates:
    return GoogleDriveCoordinates(
        path_id="folder-id",
        refresh_token="invalid-refresh-token",
        credentials=GoogleDriveCredentials(
            client_id="client-id",
            project_id="project-id",
            auth_uri="https://accounts.google.com/o/oauth2/auth",
            token_uri="https://oauth2.googleapis.com/token",
            auth_provider_x509_cert_url=("https://www.googleapis.com/oauth2/v1/certs"),
            client_secret="client-secret",
            redirect_uris=["http://localhost"],
        ),
    )


def test_google_drive_does_not_fall_back_to_browser_by_default() -> None:
    with (
        patch(
            "docling_jobkit.connectors.google_drive.helper.Credentials.refresh",
            side_effect=RefreshError("invalid_grant"),
        ),
        patch(
            "docling_jobkit.connectors.google_drive.helper."
            "InstalledAppFlow.from_client_config"
        ) as make_flow,
        pytest.raises(ConnectorAuthenticationError, match="re-authorize") as exc_info,
    ):
        get_service(_coordinates())

    assert isinstance(exc_info.value.__cause__, RefreshError)
    make_flow.assert_not_called()


def test_stored_token_refresh_failure_does_not_bypass_guard(tmp_path) -> None:
    token_path = tmp_path / "token.json"
    token_path.write_text("{}")
    stored_credentials = MagicMock(expired=True, refresh_token="invalid", valid=False)
    coords = _coordinates().model_copy(
        update={"token_path": str(token_path), "refresh_token": None}
    )

    with (
        patch(
            "docling_jobkit.connectors.google_drive.helper."
            "Credentials.from_authorized_user_file",
            return_value=stored_credentials,
        ),
        patch.object(
            stored_credentials,
            "refresh",
            side_effect=RefreshError("invalid_grant"),
        ),
        patch(
            "docling_jobkit.connectors.google_drive.helper."
            "InstalledAppFlow.from_client_config"
        ) as make_flow,
        pytest.raises(ConnectorAuthenticationError) as exc_info,
    ):
        get_service(coords)

    assert isinstance(exc_info.value.__cause__, RefreshError)
    make_flow.assert_not_called()


def test_source_processor_marks_authentication_phase() -> None:
    auth_error = ConnectorAuthenticationError("authentication failed")

    with (
        patch(
            "docling_jobkit.connectors.google_drive.helper.get_service",
            side_effect=auth_error,
        ),
        pytest.raises(SourceConnectorAuthenticationError) as exc_info,
    ):
        with GoogleDriveSourceProcessor(_coordinates()):
            pass

    assert exc_info.value.__cause__ is auth_error


def test_google_drive_browser_fallback_is_explicitly_enabled_for_cli() -> None:
    refreshed_credentials = MagicMock(valid=True)
    flow = MagicMock()
    flow.run_local_server.return_value = refreshed_credentials
    drive_service = MagicMock()

    with (
        patch(
            "docling_jobkit.connectors.google_drive.helper.Credentials.refresh",
            side_effect=RefreshError("invalid_grant"),
        ),
        patch(
            "docling_jobkit.connectors.google_drive.helper."
            "InstalledAppFlow.from_client_config",
            return_value=flow,
        ),
        patch(
            "docling_jobkit.connectors.google_drive.helper.build",
            return_value=drive_service,
        ),
        allow_interactive_auth(),
    ):
        assert get_service(_coordinates()) is drive_service

    flow.run_local_server.assert_called_once_with(port=0)
    assert not is_interactive_auth_allowed()
