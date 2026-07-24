# This module is kept for backwards-compatibility. OpenSearch models and TaskTarget
# are re-exported here so existing ``from task_targets import OpenSearch*`` imports
# keep working.

from docling_jobkit.connectors.opensearch.models import (
    OpenSearchAuth,
    OpenSearchAWSIAMAuth,
    OpenSearchBasicAuth,
    OpenSearchChunkTarget,
    OpenSearchDocTarget,
)
from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings
from docling_jobkit.datamodel.task import TaskTarget

__all__ = [
    "ChunkFieldSlots",
    "FieldMappings",
    "OpenSearchAWSIAMAuth",
    "OpenSearchAuth",
    "OpenSearchBasicAuth",
    "OpenSearchChunkTarget",
    "OpenSearchDocTarget",
    "TaskTarget",
]
