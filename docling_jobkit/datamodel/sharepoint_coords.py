from typing import Literal

from pydantic import BaseModel


class SharePointCoordinates(BaseModel):
    pass


class TaskSharePointSource(SharePointCoordinates):
    kind: Literal["sharepoint"] = "sharepoint"


__all__ = ["SharePointCoordinates", "TaskSharePointSource"]
