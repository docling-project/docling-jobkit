from typing import Any

from pydantic import BaseModel

from docling_jobkit.connectors.connector_factory import get_target_connector_factory
from docling_jobkit.connectors.target_processor import BaseTargetProcessor


def get_target_processor(
    target: BaseModel,
    *,
    allow_external_plugins: bool = False,
    **kwargs: Any,
) -> BaseTargetProcessor:
    factory = get_target_connector_factory(allow_external_plugins)
    return factory.create_instance(factory.validate_config(target), **kwargs)
