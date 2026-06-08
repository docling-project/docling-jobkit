from datetime import datetime
from io import BytesIO
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.service.sources import S3Coordinates
from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.s3_helper import get_s3_connection
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)


class S3FileIdentifier(BaseModel):
    key: str  # S3 object key
    size: int
    last_modified: datetime | None = None


class S3SourceProcessor(BaseSourceProcessor[S3Coordinates, S3FileIdentifier]):
    def __init__(self, coords: S3Coordinates):
        super().__init__(coords)
        self._coords = coords

    def _initialize(self):
        self._client, self._resource = get_s3_connection(self._coords)

    def _finalize(self):
        self._client.close()

    def _list_document_ids(self) -> Iterator[S3FileIdentifier]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self._coords.bucket,
            Prefix=self._coords.key_prefix,
        ):
            for obj in page.get("Contents", []):
                last_modified = obj.get("LastModified", None)
                yield S3FileIdentifier(
                    key=obj["Key"],  # type: ignore[typeddict-item]  # Key is always present in S3 list_objects_v2 response
                    size=obj.get("Size", 0),
                    last_modified=last_modified,
                )

    def _count_documents(self) -> int:
        total = 0
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self._coords.bucket,
            Prefix=self._coords.key_prefix,
        ):
            total += len(page.get("Contents", []))
        return total

    @override
    def _make_document_ref(
        self, identifier: S3FileIdentifier, source_index: int
    ) -> SourceDocumentRef[S3FileIdentifier]:
        key = identifier.key
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"s3://{self._coords.bucket}/{key}",
            filename=key,
        )

    # ----------------- Document fetch -----------------

    def _fetch_document_by_id(self, identifier: S3FileIdentifier) -> DocumentStream:
        buffer = BytesIO()
        self._client.download_fileobj(
            Bucket=self._coords.bucket, Key=identifier.key, Fileobj=buffer
        )
        buffer.seek(0)
        return DocumentStream(name=identifier.key, stream=buffer)

    def _fetch_documents(self):
        for key in self._list_document_ids():
            yield self._fetch_document_by_id(key)
