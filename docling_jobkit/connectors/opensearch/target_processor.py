import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.opensearch.models import (
    OpenSearchAWSIAMAuth,
    OpenSearchChunkTarget,
    OpenSearchDocTarget,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.target_field_slots import FieldMappings

if TYPE_CHECKING:
    from opensearchpy import OpenSearch

# Both OpenSearch target types inherit FieldMappings so the bound _T=FieldMappings
# constraint of BaseDatabaseTargetProcessor is satisfied.
_OpenSearchTarget = Union[OpenSearchDocTarget, OpenSearchChunkTarget]


class OpenSearchTargetProcessor(BaseDatabaseTargetProcessor[_OpenSearchTarget]):
    def __init__(self, target: _OpenSearchTarget) -> None:
        super().__init__(target)
        self._client: "Optional[OpenSearch]" = None
        self._current_document_hash: Optional[str] = None

    @classmethod
    def check_dependencies(cls) -> None:
        import opensearchpy  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (OpenSearchDocTarget, OpenSearchChunkTarget)

    def _initialize(self) -> None:
        from opensearchpy import OpenSearch

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

    # ------------------------------------------------------------------
    # Streaming chunk protocol
    # ------------------------------------------------------------------

    def instance_requires_chunks(self) -> bool:
        """True when this processor is configured as an opensearch_chunks target."""
        return isinstance(self._target, OpenSearchChunkTarget)

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: Optional[str] = None,
        document_hash: Optional[str] = None,
    ) -> None:
        """No file needed — each chunk is upserted directly in consume_chunk()."""
        self._current_document_hash = document_hash

    def consume_chunk(self, chunk: ChunkedDocumentResultItem) -> None:
        """Upsert one chunk row into OpenSearch immediately."""
        from docling_jobkit.convert.chunking import _chunk_row_payload
        from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots

        if not isinstance(self._target, ChunkFieldSlots):
            raise TypeError(
                f"opensearch_chunks processor requires a ChunkFieldSlots target, "
                f"got {type(self._target)!r}"
            )
        row = _chunk_row_payload(chunk, self._target)
        doc_id = self._stable_chunk_id(
            binary_hash=self._current_document_hash,
            filename=chunk.filename,
            chunk_index=chunk.chunk_index,
        )
        self._index_document(row, document_id=doc_id)

    def end_chunks(self) -> None:
        """Nothing to flush — each chunk was written in consume_chunk()."""

    def _is_serverless(self) -> bool:
        auth = self._target.auth
        return isinstance(auth, OpenSearchAWSIAMAuth) and auth.service == "aoss"

    def upsert_row(self, row: dict[str, Any]) -> None:
        # Use the pre-existing doc-ID field in the row when available (deterministic
        # upsert semantics), otherwise fall back to the pending doc-ID captured by
        # begin_document, then finally to a hash of the row content.
        # Only OpenSearchDocTarget declares ``id_field``; chunk targets derive
        # their IDs via _stable_chunk_id() and never reach this method.
        if (
            isinstance(self._target, OpenSearchDocTarget)
            and self._target.id_field in row
        ):
            row_id = str(row[self._target.id_field])
        elif self._pending_doc_id is not None:
            row_id = self._pending_doc_id
        else:
            row_id = self._row_content_hash(row)
        self._index_document(row, document_id=row_id)

    def _index_document(self, body: dict[str, Any], document_id: str) -> None:
        if self._client is None:
            raise RuntimeError("OpenSearchTargetProcessor is not initialized")
        # OpenSearch Serverless (AOSS) does not allow caller-specified document
        # IDs on index/create operations — omit the id in that case.
        #
        # CAVEAT: because AOSS auto-generates IDs, the deterministic IDs derived
        # by _stable_chunk_id()/upsert_row() cannot be used there. Re-ingesting
        # the same document/chunk therefore INSERTS a new record instead of
        # upserting over the previous one, i.e. idempotent re-ingestion produces
        # duplicates on Serverless. This limitation does not apply to managed
        # Amazon OpenSearch Service ('es') or self-hosted clusters.
        kwargs: dict[str, Any] = {"index": self._target.index, "body": body}
        if not self._is_serverless():
            kwargs["id"] = document_id
        self._client.index(**kwargs)

    @staticmethod
    def _stable_chunk_id(
        binary_hash: Optional[str],
        filename: str,
        chunk_index: int,
    ) -> str:
        """Derive a deterministic, content-addressed ID for a single chunk.

        The ID is a SHA-256 hex digest of ``"<binary_hash>:<filename>:<chunk_index>"``.
        Using ``binary_hash`` (SHA-256 of the raw input bytes) means the same
        file uploaded under different names still produces the same chunk IDs,
        avoiding duplicate entries on re-ingestion.  ``filename`` is included as
        a tiebreaker for the rare case where ``binary_hash`` is unavailable (falls
        back to filename-only stability).  ``chunk_index`` scopes the ID to the
        specific chunk within the document.
        """
        key = f"{binary_hash or filename}:{filename}:{chunk_index}"
        return hashlib.sha256(key.encode(), usedforsecurity=False).hexdigest()

    @staticmethod
    def _row_content_hash(row: dict[str, Any]) -> str:
        """Fallback ID for non-chunk doc rows: SHA-256 of the serialised row."""
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode(), usedforsecurity=False).hexdigest()
