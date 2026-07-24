"""Pluggy-based factory for source/target connectors.

Mirrors docling's ``docling.models.factories.base_factory`` so connectors can be
discovered through setuptools entry points (group ``docling_jobkit``) and
contributed by third-party packages, while built-in connectors keep working with
no configuration. The registry is keyed by the connector's *config* model type
(the pydantic model carrying a ``Literal`` ``kind``), and instances are created
from a config object via :meth:`BaseConnectorFactory.create_instance`.
"""

import logging
from abc import ABCMeta
from collections.abc import Mapping
from functools import lru_cache
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pluggy import PluginManager
from pydantic import BaseModel, Field
from pydantic_core import PydanticUndefined

from docling_jobkit.connectors.errors import (
    SourceConnectorConfigError,
    TargetConnectorConfigError,
)
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.connectors.target_processor import BaseTargetProcessor

logger = logging.getLogger(__name__)

PLUGIN_GROUP = "docling_jobkit"
_INTERNAL_MODULE_PREFIX = "docling_jobkit."

P = TypeVar("P", BaseSourceProcessor, BaseTargetProcessor)


class ConnectorFactoryMeta(BaseModel):
    kind: str
    plugin_name: str
    module: str


def _kind_of(config_type: type[BaseModel]) -> str:
    """Return the ``kind`` discriminator value declared on a config model."""
    field = config_type.model_fields.get("kind")
    if field is None or get_origin(field.annotation) is not Literal:
        raise ValueError(
            f"Connector config {config_type!r} must declare one non-empty string "
            "`Literal` value for `kind`."
        )
    values = get_args(field.annotation)
    if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
        raise ValueError(
            f"Connector config {config_type!r} must declare one non-empty string "
            "`Literal` value for `kind`."
        )
    kind = values[0]
    if field.default is PydanticUndefined or field.default != kind:
        raise ValueError(
            f"Connector config {config_type!r} must default `kind` to {kind!r}."
        )
    return kind


class BaseConnectorFactory(Generic[P], metaclass=ABCMeta):
    default_plugin_name = PLUGIN_GROUP

    def __init__(self, plugin_attr_name: str, plugin_name: str = default_plugin_name):
        self.plugin_name = plugin_name
        self.plugin_attr_name = (
            plugin_attr_name  # "source_connectors"/"target_connectors"
        )
        self._classes: dict[type[BaseModel], type[P]] = {}
        self._meta: dict[type[BaseModel], ConnectorFactoryMeta] = {}
        self._config_types_by_kind: dict[str, type[BaseModel]] = {}

    @property
    def registered_config_types(self) -> tuple[type[BaseModel], ...]:
        return tuple(self._classes.keys())

    @property
    def registered_kinds(self) -> list[str]:
        return list(self._config_types_by_kind)

    @property
    def registered_config_types_by_kind(self) -> dict[str, type[BaseModel]]:
        return dict(self._config_types_by_kind)

    @property
    def registered_meta(self) -> dict[type[BaseModel], ConnectorFactoryMeta]:
        return self._meta

    def register(self, cls: type[P], plugin_name: str, plugin_module_name: str) -> None:
        registrations: list[tuple[type[BaseModel], str]] = []
        for config_type in cls.get_config_types():
            kind = _kind_of(config_type)
            if config_type in self._classes:
                raise ValueError(
                    f"{kind!r} ({config_type!r}) already registered to class "
                    f"{self._classes[config_type]!r}"
                )
            if kind in self._config_types_by_kind or any(
                registered_kind == kind for _, registered_kind in registrations
            ):
                registered_type = self._config_types_by_kind.get(kind) or next(
                    registered_config
                    for registered_config, registered_kind in registrations
                    if registered_kind == kind
                )
                raise ValueError(
                    f"Connector kind {kind!r} is already registered to "
                    f"{registered_type!r}."
                )
            registrations.append((config_type, kind))

        for config_type, kind in registrations:
            self._classes[config_type] = cls
            self._config_types_by_kind[kind] = config_type
            self._meta[config_type] = ConnectorFactoryMeta(
                kind=kind,
                plugin_name=plugin_name,
                module=plugin_module_name,
            )

    def create_instance(self, config: BaseModel, **kwargs) -> P:
        cls = self._classes.get(type(config))
        if cls is None:
            raise RuntimeError(self._err_msg_on_class_not_found(config))
        return cls(config, **kwargs)  # type: ignore[call-arg]

    def build_discriminated_union(self):
        """Build a pydantic discriminated union of the registered config types.

        Returns the bare type when only one connector is registered (pydantic
        rejects a single-member discriminated union).
        """
        types = self.registered_config_types
        if not types:
            raise RuntimeError(
                f"No connectors registered for {self.plugin_attr_name!r}."
            )
        if len(types) == 1:
            return types[0]
        return Annotated[Union[types], Field(discriminator="kind")]  # type: ignore[valid-type]

    def load_from_plugins(
        self,
        plugin_name: Optional[str] = None,
        allow_external_plugins: bool = False,
    ) -> None:
        plugin_name = plugin_name or self.plugin_name

        plugin_manager = PluginManager(plugin_name)
        plugin_manager.load_setuptools_entrypoints(plugin_name)

        for entry_name, plugin_module in plugin_manager.list_name_plugin():
            plugin_module_name = str(plugin_module.__name__)  # type: ignore[attr-defined]

            if not allow_external_plugins and not plugin_module_name.startswith(
                _INTERNAL_MODULE_PREFIX
            ):
                logger.warning(
                    "The plugin %r will not be loaded because docling-jobkit is "
                    "being executed with allow_external_plugins=false.",
                    entry_name,
                )
                continue

            attr = getattr(plugin_module, self.plugin_attr_name, None)
            if callable(attr):
                logger.info("Loading connector plugin %r", entry_name)
                config = attr()
                self.process_plugin(config, entry_name, plugin_module_name)

    def process_plugin(self, config, plugin_name: str, plugin_module_name: str) -> None:
        for item in config[self.plugin_attr_name]:
            self.register(item, plugin_name, plugin_module_name)

    def _err_msg_on_class_not_found(self, config: BaseModel) -> str:
        known = "\n".join(
            f"\t{meta.kind!r} => {cls!r}"
            for cls, meta in zip(self._classes.values(), self._meta.values())
        )
        kind = getattr(config, "kind", type(config).__name__)
        return (
            f"No connector found for {kind!r} ({type(config)!r}), known "
            f"connectors are:\n{known}"
        )


class SourceConnectorFactory(BaseConnectorFactory[BaseSourceProcessor]):
    def __init__(self, plugin_name: str = PLUGIN_GROUP):
        super().__init__("source_connectors", plugin_name)

    def supports(self, kind: str) -> bool:
        return kind in self._config_types_by_kind

    def validate_config(self, payload: Mapping[str, Any] | BaseModel) -> BaseModel:
        if isinstance(payload, Mapping):
            kind = payload.get("kind")
            values: Mapping[str, Any] = payload
        elif isinstance(payload, BaseModel):
            values = payload.model_dump(mode="python")
            kind = values.get("kind")
        else:
            kind = None
            values = {}

        if not isinstance(kind, str) or not kind:
            raise SourceConnectorConfigError(
                "Source connector config requires a non-empty string `kind`."
            )

        config_type = self._config_types_by_kind.get(kind)
        if config_type is None:
            raise SourceConnectorConfigError(
                f"Source connector kind {kind!r} is not registered."
            )
        if type(payload) is config_type:
            return payload

        meta = self._meta[config_type]
        try:
            return config_type.model_validate(values)
        except Exception as exc:
            raise SourceConnectorConfigError(
                f"Source connector kind {kind!r} registered by plugin "
                f"{meta.plugin_name!r} ({meta.module}) has invalid configuration; "
                "the submitted payload may be incompatible with the locally "
                "installed plugin."
            ) from exc

    def is_expandable(self, config: Mapping[str, Any] | BaseModel) -> bool:
        normalized = self.validate_config(config)
        return self._classes[type(normalized)].is_expandable(normalized)


class TargetConnectorFactory(BaseConnectorFactory[BaseTargetProcessor]):
    def __init__(self, plugin_name: str = PLUGIN_GROUP):
        super().__init__("target_connectors", plugin_name)

    def supports(self, config: Mapping[str, Any] | BaseModel | None) -> bool:
        if config is None:
            return False
        kind = (
            config.get("kind")
            if isinstance(config, Mapping)
            else getattr(config, "kind", None)
        )
        return isinstance(kind, str) and kind in self._config_types_by_kind

    def validate_config(self, payload: Mapping[str, Any] | BaseModel) -> BaseModel:
        values = (
            payload
            if isinstance(payload, Mapping)
            else payload.model_dump(mode="python")
            if isinstance(payload, BaseModel)
            else {}
        )
        kind = values.get("kind")
        if not isinstance(kind, str) or not kind:
            raise TargetConnectorConfigError(
                "Target connector config requires a non-empty string `kind`."
            )

        config_type = self._config_types_by_kind.get(kind)
        if config_type is None:
            raise TargetConnectorConfigError(
                f"Target connector kind {kind!r} is not registered."
            )
        if type(payload) is config_type:
            return payload

        try:
            return config_type.model_validate(values)
        except Exception as exc:
            meta = self._meta[config_type]
            raise TargetConnectorConfigError(
                f"Target connector kind {kind!r} registered by plugin "
                f"{meta.plugin_name!r} ({meta.module}) has invalid configuration."
            ) from exc

    def result_mode(
        self, config: Mapping[str, Any] | BaseModel | None
    ) -> Literal["artifacts", "archive", "presigned", "database"]:
        if config is None:
            raise TargetConnectorConfigError("result_mode called with no target config")
        normalized = self.validate_config(config)
        return self._classes[type(normalized)].result_mode()

    def result_mode_for_kind(
        self, kind: str
    ) -> Literal["artifacts", "archive", "presigned", "database"]:
        config_type = self._config_types_by_kind.get(kind)
        if config_type is None:
            raise TargetConnectorConfigError(
                f"Target connector kind {kind!r} is not registered."
            )
        return self._classes[config_type].result_mode()


@lru_cache(maxsize=None)
def get_source_connector_factory(
    allow_external_plugins: bool = False,
) -> SourceConnectorFactory:
    factory = SourceConnectorFactory()
    factory.load_from_plugins(allow_external_plugins=allow_external_plugins)
    return factory


@lru_cache(maxsize=None)
def get_target_connector_factory(
    allow_external_plugins: bool = False,
) -> TargetConnectorFactory:
    factory = TargetConnectorFactory()
    factory.load_from_plugins(allow_external_plugins=allow_external_plugins)
    return factory
