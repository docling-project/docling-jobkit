"""AstraDB connector for chunk-level vector storage with server-side vectorization."""

from docling_jobkit.connectors.astradb.models import AstraDBChunkTarget
from docling_jobkit.connectors.astradb.target_processor import (
    AstraDBTargetProcessor,
)

__all__ = [
    "AstraDBChunkTarget",
    "AstraDBTargetProcessor",
]
