from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter

from docling.datamodel.service.responses import PublicFailureInfo

from docling_jobkit.datamodel.result import DoclingTaskResult


class StoredSuccessOutcome(BaseModel):
    kind: Literal["success"] = "success"
    result: DoclingTaskResult


class StoredFailureOutcome(BaseModel):
    kind: Literal["failure"] = "failure"
    failure: PublicFailureInfo


StoredTaskOutcome = Annotated[
    StoredSuccessOutcome | StoredFailureOutcome,
    Field(discriminator="kind"),
]
stored_task_outcome_adapter: TypeAdapter[StoredTaskOutcome] = TypeAdapter(
    StoredTaskOutcome
)
