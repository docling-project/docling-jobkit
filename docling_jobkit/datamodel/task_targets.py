from typing import Annotated, Literal

from pydantic import BaseModel, Field


class InBodyTarget(BaseModel):
    kind: Literal["inbody"] = "inbody"


class ZipTarget(BaseModel):
    kind: Literal["zip"] = "zip"


TaskTarget = Annotated[InBodyTarget | ZipTarget, Field(discriminator="kind")]
