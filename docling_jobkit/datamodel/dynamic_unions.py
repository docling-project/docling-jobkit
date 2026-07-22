"""Build pydantic discriminated unions from the registered connector plugins.

The static source and target unions cover built-in CLI/YAML configuration. These
helpers build precise plugin-aware unions for that external configuration boundary.
Internal task sources and targets hydrate structurally through the connector registry.
"""

from docling_jobkit.connectors.connector_factory import (
    get_source_connector_factory,
    get_target_connector_factory,
)


def build_source_union(allow_external_plugins: bool = False):
    """Discriminated union of all registered source-connector config models."""
    return get_source_connector_factory(
        allow_external_plugins
    ).build_discriminated_union()


def build_target_union(allow_external_plugins: bool = False):
    """Discriminated union of all registered target-connector config models."""
    return get_target_connector_factory(
        allow_external_plugins
    ).build_discriminated_union()


def build_job_config_model(allow_external_plugins: bool = False):
    """Build a CLI ``JobConfig``-shaped model whose source/target accept plugins."""
    from typing import Annotated, Union

    from pydantic import ConfigDict, Field, create_model

    from docling.datamodel.service.options import ConvertDocumentsOptions
    from docling.datamodel.service.targets import PresignedUrlTarget, ZipTarget

    source_union = build_source_union(allow_external_plugins)
    target_types = (
        *get_target_connector_factory(allow_external_plugins).registered_config_types,
        ZipTarget,
    )
    target_types = tuple(t for t in target_types if t is not PresignedUrlTarget)
    target_union = Annotated[Union[target_types], Field(discriminator="kind")]  # type: ignore[valid-type]

    return create_model(
        "DynamicJobConfig",
        __config__=ConfigDict(arbitrary_types_allowed=True),
        options=(ConvertDocumentsOptions, ConvertDocumentsOptions()),
        sources=(list[source_union], ...),  # type: ignore[valid-type]
        target=(target_union, ...),  # type: ignore[valid-type]
    )


# Re-exported for callers that only need the optional set helper.
__all__ = [
    "build_job_config_model",
    "build_source_union",
    "build_target_union",
]
