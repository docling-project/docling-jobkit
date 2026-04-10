from typing import Annotated

from pydantic import BaseModel, Field

from docling.datamodel.service.chunking import (
    BaseChunkerOptions,
    ChunkerType,
    HierarchicalChunkerOptions,
    HybridChunkerOptions,
)


class ChunkingExportOptions(BaseModel):
    """Export options for the response of the chunking task."""

    include_converted_doc: bool = False


ChunkingOptionType = Annotated[
    HybridChunkerOptions | HierarchicalChunkerOptions,
    Field(discriminator="chunker"),
]

__all__ = [
    "BaseChunkerOptions",
    "ChunkerType",
    "ChunkingExportOptions",
    "ChunkingOptionType",
    "HierarchicalChunkerOptions",
    "HybridChunkerOptions",
]
