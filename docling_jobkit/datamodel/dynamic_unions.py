"""Build pydantic discriminated unions from the registered connector plugins.

The static unions in :mod:`docling_jobkit.datamodel.task_sources` /
:mod:`docling_jobkit.datamodel.task_targets` cover the built-in connectors. When
third-party connectors are enabled (``allow_external_plugins=True``), their config
models must also be accepted when validating job configs (CLI) and when a worker
re-validates a ``Task`` (RQ). These helpers build those unions from the connector
factories so external ``kind`` values parse.

``install_dynamic_unions`` must run once at process entry, before any ``Task`` is
validated, with the same ``allow_external_plugins`` value as the submitter.
"""

import logging

from docling_jobkit.connectors.connector_factory import (
    get_source_connector_factory,
    get_target_connector_factory,
)

logger = logging.getLogger(__name__)


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


def install_dynamic_unions(allow_external_plugins: bool = False) -> None:
    """Rebind ``Task.target`` to include externally-registered target connectors.

    With ``allow_external_plugins=False`` the rebuilt union contains exactly the
    built-in connectors, so this is a no-op equivalent to the static annotation.
    Service-only targets (in-body / zip / presigned) are preserved because they
    remain part of the original ``TaskTarget`` annotation that this union extends.
    """
    if not allow_external_plugins:
        return

    # Import here to avoid an import cycle (task -> task_targets) and to keep the
    # entry-point scan out of module import time.
    from typing import Annotated, Union

    from pydantic import Field

    from docling.datamodel.service.targets import (
        InBodyTarget,
        PresignedUrlTarget,
        PutTarget,
        S3Target,
        ZipTarget,
    )

    from docling_jobkit.datamodel.task import Task
    from docling_jobkit.datamodel.task_targets import (
        GoogleDriveTarget,
        LocalPathTarget,
    )

    factory = get_target_connector_factory(allow_external_plugins)
    # Service-only targets are never registered as connectors but must stay valid.
    service_only = (InBodyTarget, ZipTarget, PresignedUrlTarget)
    builtin = (S3Target, PutTarget, LocalPathTarget, GoogleDriveTarget)
    external = tuple(
        t
        for t in factory.registered_config_types
        if t not in builtin and t not in service_only
    )
    if not external:
        return

    members = (*service_only, *builtin, *external)
    new_union = Annotated[Union[members], Field(discriminator="kind")]  # type: ignore[valid-type]

    Task.model_fields["target"].annotation = new_union  # type: ignore[assignment]
    Task.model_rebuild(force=True)
    logger.info(
        "Installed dynamic target union with external kinds: %s",
        [factory.registered_meta[t].kind for t in external],
    )


def build_job_config_model(allow_external_plugins: bool = False):
    """Build a CLI ``JobConfig``-shaped model whose source/target accept plugins."""
    from pydantic import ConfigDict, create_model

    from docling.datamodel.service.options import ConvertDocumentsOptions

    source_union = build_source_union(allow_external_plugins)
    target_union = build_target_union(allow_external_plugins)

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
    "install_dynamic_unions",
]
