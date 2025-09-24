from typing import Annotated, Literal

from pydantic import Field

from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class TaskFileSource(FileSource):
    kind: Literal["file"] = "file"


class TaskHttpSource(HttpSource):
    kind: Literal["http"] = "http"


class TaskS3Source(S3Coordinates):
    kind: Literal["s3"] = "s3"


TaskSource = Annotated[
    TaskFileSource | TaskHttpSource | TaskS3Source, Field(discriminator="kind")
]
