from datetime import datetime

from pydantic import BaseModel


class GoogleCloudStorageFileIdentifier(BaseModel):
    # TODO: need to investigate if theres other metadata
    name: str
    size: int | None = None
    last_modified: datetime | None = None
