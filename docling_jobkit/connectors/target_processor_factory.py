from typing import Any, Optional

from docling_jobkit.connectors.connector_factory import get_target_connector_factory
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task_targets import TaskTarget


def get_target_processor(
    target: TaskTarget,
    *,
    allow_external_plugins: bool = False,
    chunking_options: Optional[Any] = None,
) -> BaseTargetProcessor:
    """Instantiate the target processor for ``target`` via the connector factory.

    Thin backward-compatible wrapper: dispatch is now driven by the pluggy-based
    :class:`TargetConnectorFactory` (keyed on the config model's ``kind``), so new
    target connectors are added by registering a plugin rather than editing this
    function. Service-only targets (in-body / zip / presigned) are handled by the
    result-export path and are intentionally not registered here.

    ``chunking_options`` is forwarded to processors that support it (e.g.
    :class:`AstraDBTargetProcessor`); processors that do not accept it are
    instantiated without it.
    """
    factory = get_target_connector_factory(allow_external_plugins)
    kwargs: dict[str, Any] = {}
    if chunking_options is not None:
        kwargs["chunking_options"] = chunking_options
    try:
        return factory.create_instance(target, **kwargs)
    except TypeError:
        # Processor does not accept chunking_options; fall back without it.
        return factory.create_instance(target)
