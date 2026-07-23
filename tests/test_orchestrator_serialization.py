from docling.datamodel.service.requests import (
    GoogleCloudStorageSourceRequest,
    GoogleDriveSourceRequest,
)
from docling.datamodel.service.sources import (
    GoogleCloudStorageServiceAccountInfo,
    GoogleDriveCredentials,
)
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.serialization import (
    dump_model_with_secrets,
)


def test_dump_model_with_secrets_restores_gcs_credentials():
    task = Task(
        task_id="task-gcs",
        task_type=TaskType.CONVERT,
        sources=[
            GoogleCloudStorageSourceRequest(
                bucket="bucket",
                key_prefix="prefix",
                service_account_key=GoogleCloudStorageServiceAccountInfo(
                    project_id="project-id",
                    private_key_id="key-id",
                    private_key="-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n",
                    client_email="svc@example.iam.gserviceaccount.com",
                    client_id="client-id",
                    auth_uri="https://accounts.google.com/o/oauth2/auth",
                    token_uri="https://oauth2.googleapis.com/token",
                    auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
                    client_x509_cert_url="https://www.googleapis.com/robot/v1/metadata/x509/svc%40example",
                    universe_domain="googleapis.com",
                ),
            )
        ],
    )

    payload = dump_model_with_secrets(task)

    assert (
        payload["sources"][0]["service_account_key"]["private_key"]
        == "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n"
    )
    assert payload["sources"][0]["service_account_key"]["private_key"] != "**********"
    assert (
        payload["sources"][0]["service_account_key"]["client_email"]
        == "svc@example.iam.gserviceaccount.com"
    )


def test_dump_model_with_secrets_restores_google_drive_credentials():
    task = Task(
        task_id="task-drive",
        task_type=TaskType.CONVERT,
        sources=[
            GoogleDriveSourceRequest(
                path_id="drive-folder-id",
                token_path="/tmp/token.json",
                credentials=GoogleDriveCredentials(
                    client_id="client-id",
                    project_id="project-id",
                    auth_uri="https://accounts.google.com/o/oauth2/auth",
                    token_uri="https://oauth2.googleapis.com/token",
                    auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
                    client_secret="super-secret",
                    redirect_uris=["http://localhost"],
                ),
            )
        ],
    )

    payload = dump_model_with_secrets(task, serialize_as_any=True)

    assert payload["sources"][0]["credentials"]["client_secret"] == "super-secret"
