from pydantic import BaseModel, Field


class CallbackEmissionPolicy(BaseModel):
    """Internal policy controlling which callback kinds a worker may emit."""

    emit_set_num_docs: bool = Field(default=True)
    emit_document_completed: bool = Field(default=True)
    emit_update_processed: bool = Field(default=True)
