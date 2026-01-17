from io import BytesIO
from typing import Iterator, TypedDict

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.s3_helper import get_s3_connection
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3FileIdentifier(TypedDict):
    key: str  # S3 object key
    size: int  # optional, include if available
    last_modified: str | None  # ISO timestamp, optional


class S3SourceProcessor(BaseSourceProcessor[S3FileIdentifier]):
    def __init__(self, coords: S3Coordinates):
        super().__init__()
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
                    last_modified=last_modified.isoformat() if last_modified else None,
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

    # ----------------- Document fetch -----------------

    def _fetch_document_by_id(self, identifier: S3FileIdentifier) -> DocumentStream:
        buffer = BytesIO()
        self._client.download_fileobj(
            Bucket=self._coords.bucket, Key=identifier["key"], Fileobj=buffer
        )
        buffer.seek(0)
        return DocumentStream(name=identifier["key"], stream=buffer)

    def _fetch_documents(self):
        for key in self._list_document_ids():
            yield self._fetch_document_by_id(key)
