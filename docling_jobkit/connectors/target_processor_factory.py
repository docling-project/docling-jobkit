from docling_jobkit.connectors.connector_factory import get_target_connector_factory
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task_targets import ChunkTarget, TaskTarget


def get_target_processor(
    target: TaskTarget | ChunkTarget,
    *,
    allow_external_plugins: bool = False,
) -> BaseTargetProcessor:
    """Instantiate the target processor for ``target`` via the connector factory.

    Thin backward-compatible wrapper: dispatch is now driven by the pluggy-based
    :class:`TargetConnectorFactory` (keyed on the config model's ``kind``), so new
    target connectors are added by registering a plugin rather than editing this
    function. Service-only targets (in-body / zip / presigned) are handled by the
    result-export path and are intentionally not registered here.
    """
    factory = get_target_connector_factory(allow_external_plugins)
    return factory.create_instance(target)
