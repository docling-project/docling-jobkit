from typing import Literal

from pydantic import BaseModel

from docling_jobkit.datamodel.s3_inputs import S3Coordinates


class InBodyTarget(BaseModel):
    kind: Literal["inbody"] = "inbody"


class ZipTarget(BaseModel):
    kind: Literal["zip"] = "zip"


class S3Target(S3Coordinates):
    kind: Literal["s3"] = "s3"
