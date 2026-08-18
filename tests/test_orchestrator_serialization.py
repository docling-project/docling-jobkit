from docling.datamodel.service.requests import (
    GoogleCloudStorageSourceRequest,
    GoogleDriveSourceRequest,
)
from docling.datamodel.service.sources import (
    GoogleCloudStorageServiceAccountInfo,
    GoogleDriveCredentials,
)
from docling.datamodel.service.targets import InBodyTarget
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.serialization import (
    dump_model_with_secrets,
)


def test_dump_model_with_secrets_restores_gcs_credentials():
    task = Task(
        task_id="task-gcs",
        task_type=TaskType.CONVERT,
        target=InBodyTarget(),
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
        target=InBodyTarget(),
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


def test_dump_model_preserves_api_engine_options_subclass_fields():
    """Ray Redis enqueue uses serialize_as_any=True; engine_options subclass fields must survive.

    Regression test for: ApiVlmEngineOptions.url and .params being silently dropped
    when the task is serialized without serialize_as_any=True because the declared
    field type is BaseVlmEngineOptions (the base class), causing Pydantic to use only
    the base-class schema for serialization and discarding all subclass-only fields.
    """

    from docling.datamodel.pipeline_options import (
        PictureDescriptionVlmEngineOptions,
        ResponseFormat,
        VlmModelSpec,
    )
    from docling.datamodel.service.options import ConvertDocumentsOptions
    from docling.datamodel.service.targets import InBodyTarget
    from docling.datamodel.service.tasks import TaskType
    from docling.datamodel.vlm_engine_options import ApiVlmEngineOptions

    from docling_jobkit.datamodel.task import Task

    task = Task(
        task_id="task-api-engine",
        task_type=TaskType.CONVERT,
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(
            do_picture_description=True,
            picture_description_custom_config=PictureDescriptionVlmEngineOptions(
                engine_options=ApiVlmEngineOptions(
                    url="http://127.0.0.1:8881/v1/chat/completions",
                    params={"model": "granite-vision-4-1-4b"},
                ),
                model_spec=VlmModelSpec(
                    name="granite-vision-4-1-4b",
                    default_repo_id="ibm-granite/granite-vision-4.1-4b",
                    prompt="Describe this image.",
                    response_format=ResponseFormat.PLAINTEXT,
                ),
            ),
        ),
    )

    # This is what Ray Redis enqueue does; serialize_as_any=True is required so
    # that Pydantic serialises the concrete ApiVlmEngineOptions subclass (with
    # url, params, …) rather than just the BaseVlmEngineOptions base fields.
    payload = dump_model_with_secrets(task, serialize_as_any=True)
    engine_opts_dict = payload["convert_options"]["picture_description_custom_config"][
        "engine_options"
    ]

    # url and params must survive the serialization round-trip
    assert engine_opts_dict["url"] == "http://127.0.0.1:8881/v1/chat/completions"
    assert engine_opts_dict["params"] == {"model": "granite-vision-4-1-4b"}
    assert engine_opts_dict["engine_type"] == "api"

    # Without serialize_as_any the subclass fields are silently dropped
    payload_no_any = dump_model_with_secrets(task, serialize_as_any=False)
    engine_opts_base = payload_no_any["convert_options"][
        "picture_description_custom_config"
    ]["engine_options"]
    assert "url" not in engine_opts_base, (
        "Without serialize_as_any=True, url should be missing (demonstrating the bug)"
    )
