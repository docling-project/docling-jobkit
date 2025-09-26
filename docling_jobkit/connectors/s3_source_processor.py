from io import BytesIO
from typing import Iterator

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.s3_helper import get_s3_connection, get_source_files
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3SourceProcessor(BaseSourceProcessor):
    def __init__(self, coords: S3Coordinates):
        super().__init__()
        self._coords = coords

    def _initialize(self):
        self._client, self._resource = get_s3_connection(self._coords)

    def _finalize(self):
        self._client.close()

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        # get list of object_keys
        object_keys = get_source_files(
            s3_source_client=self._client,
            s3_source_resource=self._resource,
            s3_coords=self._coords,
        )

        # download and yield one document at the time
        for obj_key in object_keys:
            # todo. stream is BytesIO
            buffer = BytesIO()
            self._client.download_fileobj(
                Bucket=self._coords.bucket,
                Key=obj_key,
                Fileobj=buffer,
            )
            buffer.seek(0)
            yield DocumentStream(
                name=obj_key,
                stream=buffer,
            )
