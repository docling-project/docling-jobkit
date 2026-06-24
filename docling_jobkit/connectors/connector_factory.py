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
from functools import lru_cache
from typing import Annotated, Generic, Optional, TypeVar, Union

from pluggy import PluginManager
from pydantic import BaseModel, Field

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
    if field is None or field.default is None:
        raise ValueError(
            f"Connector config {config_type!r} must declare a `kind` field with a "
            "literal default to be registered."
        )
    return str(field.default)


class BaseConnectorFactory(Generic[P], metaclass=ABCMeta):
    default_plugin_name = PLUGIN_GROUP

    def __init__(self, plugin_attr_name: str, plugin_name: str = default_plugin_name):
        self.plugin_name = plugin_name
        self.plugin_attr_name = (
            plugin_attr_name  # "source_connectors"/"target_connectors"
        )
        self._classes: dict[type[BaseModel], type[P]] = {}
        self._meta: dict[type[BaseModel], ConnectorFactoryMeta] = {}

    @property
    def registered_config_types(self) -> tuple[type[BaseModel], ...]:
        return tuple(self._classes.keys())

    @property
    def registered_kinds(self) -> list[str]:
        return [meta.kind for meta in self._meta.values()]

    @property
    def registered_meta(self) -> dict[type[BaseModel], ConnectorFactoryMeta]:
        return self._meta

    def register(self, cls: type[P], plugin_name: str, plugin_module_name: str) -> None:
        for config_type in cls.get_config_types():
            kind = _kind_of(config_type)
            if config_type in self._classes:
                raise ValueError(
                    f"{kind!r} ({config_type!r}) already registered to class "
                    f"{self._classes[config_type]!r}"
                )
            self._classes[config_type] = cls
            self._meta[config_type] = ConnectorFactoryMeta(
                kind=kind,
                plugin_name=plugin_name,
                module=plugin_module_name,
            )

    def _resolve_class(self, config: BaseModel) -> Optional[type[P]]:
        # 1. Exact config-type match (the discriminated-union / YAML path).
        cls = self._classes.get(type(config))
        if cls is not None:
            return cls
        # 2. config is an instance of a registered (super)type.
        for config_type, candidate in self._classes.items():
            if isinstance(config, config_type):
                return candidate
        # 3. A bare base object (e.g. S3Coordinates) was passed but the registry
        #    holds the kind-bearing subclass (e.g. S3SourceRequest). Match when the
        #    registered type derives from the runtime type.
        for config_type, candidate in self._classes.items():
            if issubclass(config_type, type(config)):
                return candidate
        return None

    def create_instance(self, config: BaseModel, **kwargs) -> P:
        cls = self._resolve_class(config)
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
            try:
                self.register(item, plugin_name, plugin_module_name)
            except ValueError:
                logger.warning("%r already registered", item)

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


class TargetConnectorFactory(BaseConnectorFactory[BaseTargetProcessor]):
    def __init__(self, plugin_name: str = PLUGIN_GROUP):
        super().__init__("target_connectors", plugin_name)


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
