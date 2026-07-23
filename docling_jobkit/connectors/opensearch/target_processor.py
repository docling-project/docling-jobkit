import json
from typing import Any, Union

from opensearchpy import OpenSearch

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.opensearch.models import (
    OpenSearchChunkTarget,
    OpenSearchDocTarget,
)
from docling_jobkit.datamodel.target_field_slots import FieldMappings

# Both OpenSearch target types inherit FieldMappings so the bound _T=FieldMappings
# constraint of BaseDatabaseTargetProcessor is satisfied.
_OpenSearchTarget = Union[OpenSearchDocTarget, OpenSearchChunkTarget]


class OpenSearchTargetProcessor(BaseDatabaseTargetProcessor[_OpenSearchTarget]):
    def __init__(self, target: _OpenSearchTarget) -> None:
        super().__init__(target)
        self._client: OpenSearch | None = None

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (OpenSearchDocTarget, OpenSearchChunkTarget)

    def _initialize(self) -> None:
        kwargs: dict[str, Any] = {
            "hosts": self._target.hosts,
            "use_ssl": self._target.use_ssl,
            "verify_certs": self._target.verify_certs,
        }

        auth = self._target.auth
        if auth is not None and auth.kind == "basic":
            kwargs["http_auth"] = (auth.username, auth.password)
        elif auth is not None and auth.kind == "aws_iam":
            import boto3
            from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

            session = boto3.Session(
                aws_access_key_id=auth.aws_access_key_id,
                aws_secret_access_key=auth.aws_secret_access_key,
                aws_session_token=auth.aws_session_token,
                region_name=auth.region,
            )
            if auth.assume_role_arn:
                sts = session.client("sts")
                assumed = sts.assume_role(
                    RoleArn=auth.assume_role_arn,
                    RoleSessionName="docling-opensearch",
                )
                c = assumed["Credentials"]
                session = boto3.Session(
                    aws_access_key_id=c["AccessKeyId"],
                    aws_secret_access_key=c["SecretAccessKey"],
                    aws_session_token=c["SessionToken"],
                    region_name=auth.region,
                )
            credentials = session.get_credentials()
            kwargs["http_auth"] = AWSV4SignerAuth(
                credentials, auth.region, auth.service
            )
            kwargs["connection_class"] = RequestsHttpConnection

        self._client = OpenSearch(**kwargs)

    def _finalize(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # upload_file / upload_object accumulation is handled by BaseDatabaseTargetProcessor.
    # upsert_row is called once per document by end_document() with the merged row.

    def _is_serverless(self) -> bool:
        auth = self._target.auth
        return (
            auth is not None and auth.kind == "aws_iam" and auth.service == "aoss"  # type: ignore[union-attr]
        )

    def upsert_row(self, row: dict[str, Any]) -> None:
        # Use the pre-existing doc-ID field in the row when available (deterministic
        # upsert semantics), otherwise fall back to the pending doc-ID captured by
        # begin_document, then finally to a hash of the row content.
        id_field = getattr(self._target, "id_field", None)
        if id_field is not None and id_field in row:
            row_id = str(row[id_field])
        elif self._pending_doc_id is not None:
            row_id = self._pending_doc_id
        else:
            row_id = self._row_hash(row)
        self._index_document(row, document_id=row_id)

    def _index_document(self, body: dict[str, Any], document_id: str) -> None:
        if self._client is None:
            raise RuntimeError("OpenSearchTargetProcessor is not initialized")
        # OpenSearch Serverless (AOSS) does not allow caller-specified document
        # IDs on index/create operations — omit the id in that case.
        kwargs: dict[str, Any] = {"index": self._target.index, "body": body}
        if not self._is_serverless():
            kwargs["id"] = document_id
        self._client.index(**kwargs)

    @staticmethod
    def _row_hash(row: dict[str, Any]) -> str:
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        return payload.encode("utf-8").hex()[:40]
