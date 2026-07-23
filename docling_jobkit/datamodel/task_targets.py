# This module is kept for backwards-compatibility. The static discriminated unions
# (DocumentTarget, ChunkTarget, TaskTarget) have moved to task.py where they are
# resolved dynamically via the connector registry. OpenSearch models are re-exported
# here so existing ``from task_targets import OpenSearch*`` imports keep working.

from docling_jobkit.connectors.opensearch.models import (
    OpenSearchAuth,
    OpenSearchAWSIAMAuth,
    OpenSearchBasicAuth,
    OpenSearchChunkTarget,
    OpenSearchDocTarget,
)
from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings

# ChunkTarget / TaskTarget now live in task.py — re-export so existing imports work.
from docling_jobkit.datamodel.task import ChunkTarget, TaskTarget

__all__ = [
    "ChunkFieldSlots",
    "ChunkTarget",
    "FieldMappings",
    "OpenSearchAWSIAMAuth",
    "OpenSearchAuth",
    "OpenSearchBasicAuth",
    "OpenSearchChunkTarget",
    "OpenSearchDocTarget",
    "TaskTarget",
]
