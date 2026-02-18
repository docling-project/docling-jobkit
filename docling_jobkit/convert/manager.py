import enum
import hashlib
import json
import logging
import re
import sys
import threading
from collections.abc import Iterable, Iterator, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Type, TypedDict, Union

from pydantic import BaseModel, Field

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.backend.pdf_backend import PdfDocumentBackend
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel import vlm_model_specs
from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import (
    CodeFormulaVlmOptions,
    OcrOptions,
    PdfBackend,
    PdfPipelineOptions,
    PictureDescriptionApiOptions,
    PictureDescriptionVlmOptions,
    ProcessingPipeline,
    TableFormerMode,
    TableStructureOptions,
    VlmConvertOptions,
    VlmPipelineOptions,
    normalize_pdf_backend,
)
from docling.datamodel.pipeline_options_vlm_model import ApiVlmOptions, InlineVlmOptions
from docling.datamodel.vlm_engine_options import (
    ApiVlmEngineOptions,
    AutoInlineVlmEngineOptions,
    BaseVlmEngineOptions,
    MlxVlmEngineOptions,
    TransformersVlmEngineOptions,
    VllmVlmEngineOptions,
)
from docling.document_converter import (
    DocumentConverter,
    FormatOption,
    ImageFormatOption,
    PdfFormatOption,
)
from docling.models.factories import (
    get_layout_factory,
    get_ocr_factory,
    get_table_structure_factory,
)
from docling.models.inference_engines.vlm.base import VlmEngineType
from docling.pipeline.vlm_pipeline import VlmPipeline
from docling_core.types.doc import ImageRefMode

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions

_log = logging.getLogger(__name__)


# Type definitions for preset registries
class DoclingPresetInfo(TypedDict):
    """Info for a Docling built-in preset."""

    source: Literal["docling"]
    preset_id: str


class VlmCustomPresetInfo(TypedDict):
    """Info for a custom VLM preset."""

    source: Literal["custom"]
    options: VlmConvertOptions


class PictureDescriptionCustomPresetInfo(TypedDict):
    """Info for a custom picture description preset."""

    source: Literal["custom"]
    options: PictureDescriptionVlmOptions


class CodeFormulaCustomPresetInfo(TypedDict):
    """Info for a custom code/formula preset."""

    source: Literal["custom"]
    options: CodeFormulaVlmOptions


# Registry-specific types
VlmPresetInfo = Union[DoclingPresetInfo, VlmCustomPresetInfo]
PictureDescriptionPresetInfo = Union[
    DoclingPresetInfo, PictureDescriptionCustomPresetInfo
]
CodeFormulaPresetInfo = Union[DoclingPresetInfo, CodeFormulaCustomPresetInfo]

# Generic preset info type for shared functions
AnyPresetInfo = Union[
    VlmPresetInfo, PictureDescriptionPresetInfo, CodeFormulaPresetInfo
]


class DoclingConverterManagerConfig(BaseModel):
    artifacts_path: Optional[Path] = None
    options_cache_size: int = 2
    enable_remote_services: bool = False
    allow_external_plugins: bool = False

    max_num_pages: int = sys.maxsize
    max_file_size: int = sys.maxsize

    # Threading pipeline
    queue_max_size: Optional[int] = None
    ocr_batch_size: Optional[int] = None
    layout_batch_size: Optional[int] = None
    table_batch_size: Optional[int] = None
    batch_polling_interval_seconds: Optional[float] = None

    # === NEW: Preset and Engine Control ===

    # VLM Pipeline Control
    default_vlm_preset: str = Field(
        default="granite_docling",
        description='Default VLM preset to use when user specifies "default".',
    )
    allowed_vlm_presets: Optional[list[str]] = Field(
        default=None,
        description="List of allowed VLM preset IDs. None means all presets are allowed.",
    )
    custom_vlm_presets: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom VLM presets defined by admin. Maps preset ID to VlmConvertOptions.",
    )
    allowed_vlm_engines: Optional[list[str]] = Field(
        default=None,
        description="List of allowed VLM engine types. None means all engines are allowed.",
    )
    allow_custom_vlm_config: bool = Field(
        default=False,
        description="Whether users can specify custom VLM engine configurations.",
    )

    # Picture Description Control
    default_picture_description_preset: str = Field(
        default="smolvlm",
        description='Default picture description preset to use when user specifies "default".',
    )
    allowed_picture_description_presets: Optional[list[str]] = Field(
        default=None,
        description="List of allowed picture description preset IDs. None means all are allowed.",
    )
    custom_picture_description_presets: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom picture description presets. Maps preset ID to PictureDescriptionVlmOptions.",
    )
    allowed_picture_description_engines: Optional[list[str]] = Field(
        default=None,
        description="List of allowed picture description engine types. None means all are allowed.",
    )
    allow_custom_picture_description_config: bool = Field(
        default=False,
        description="Whether users can specify custom picture description configurations.",
    )

    # Code/Formula Control
    default_code_formula_preset: str = Field(
        default="default",
        description='Default code/formula preset to use when user specifies "default".',
    )
    allowed_code_formula_presets: Optional[list[str]] = Field(
        default=None,
        description="List of allowed code/formula preset IDs. None means all are allowed.",
    )
    custom_code_formula_presets: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom code/formula presets. Maps preset ID to CodeFormulaVlmOptions.",
    )
    allowed_code_formula_engines: Optional[list[str]] = Field(
        default=None,
        description="List of allowed code/formula engine types. None means all are allowed.",
    )
    allow_custom_code_formula_config: bool = Field(
        default=False,
        description="Whether users can specify custom code/formula configurations.",
    )

    # === NEW: Kind Selection Control ===

    # Table Structure Control
    default_table_structure_kind: str = Field(
        default="docling_tableformer",
        description=(
            "Default table structure kind used when user doesn't provide custom config. "
            "This kind will use preset/default configuration."
        ),
    )

    allowed_table_structure_kinds: Optional[list[str]] = Field(
        default=None,
        description=(
            "List of allowed table structure kinds (pre-filter before factory). "
            "If None, all kinds from factory are allowed. "
            "The default_table_structure_kind is always implicitly allowed. "
            "Use this to block specific plugin kinds for security or policy reasons. "
            "Users can provide custom configs for any allowed kind."
        ),
        examples=[["docling_tableformer", "approved_plugin_kind"]],
    )

    # Layout Control
    default_layout_kind: str = Field(
        default="docling_layout_default",
        description=(
            "Default layout kind used when user doesn't provide custom config. "
            "This kind will use preset/default configuration."
        ),
    )

    allowed_layout_kinds: Optional[list[str]] = Field(
        default=None,
        description=(
            "List of allowed layout kinds (pre-filter before factory). "
            "If None, all kinds from factory are allowed. "
            "The default_layout_kind is always implicitly allowed. "
            "Use this to block specific plugin kinds for security or policy reasons. "
            "Users can provide custom configs for any allowed kind."
        ),
        examples=[
            [
                "docling_layout_default",
                "layout_object_detection",
                "approved_plugin_kind",
            ]
        ],
    )


# Custom serializer for PdfFormatOption
# (model_dump_json does not work with some classes)
def _hash_pdf_format_option(pdf_format_option: PdfFormatOption) -> bytes:
    data = pdf_format_option.model_dump(serialize_as_any=True)

    # pipeline_options are not fully serialized by model_dump, dedicated pass
    if pdf_format_option.pipeline_options:
        data["pipeline_options"] = pdf_format_option.pipeline_options.model_dump(
            serialize_as_any=True, mode="json"
        )
        data["pipeline_options_type"] = (
            f"{pdf_format_option.pipeline_options.__class__.__module__}."
            f"{pdf_format_option.pipeline_options.__class__.__qualname__}"
        )
    else:
        data["pipeline_options_type"] = None

    # Replace `pipeline_cls` with a string representation
    pipeline_cls = pdf_format_option.pipeline_cls
    data["pipeline_cls"] = (
        f"{pipeline_cls.__module__}.{pipeline_cls.__qualname__}"
        if pipeline_cls is not None
        else "None"
    )

    # Replace `backend` with a string representation
    backend = pdf_format_option.backend
    data["backend"] = (
        f"{backend.__module__}.{backend.__qualname__}"
        if backend is not None
        else "None"
    )

    # Serialize the dictionary to JSON with sorted keys to have consistent hashes
    serialized_data = json.dumps(data, sort_keys=True)
    options_hash = hashlib.sha1(
        serialized_data.encode(), usedforsecurity=False
    ).digest()
    return options_hash


def _to_list_of_strings(input_value: Union[str, list[str]]) -> list[str]:
    def split_and_strip(value: str) -> list[str]:
        if re.search(r"[;,]", value):
            return [item.strip() for item in re.split(r"[;,]", value)]
        else:
            return [value.strip()]

    if isinstance(input_value, str):
        return split_and_strip(input_value)
    elif isinstance(input_value, list):
        result = []
        for item in input_value:
            result.extend(split_and_strip(str(item)))
        return result
    else:
        raise ValueError("Invalid input: must be a string or a list of strings.")


class DoclingConverterManager:
    def __init__(self, config: DoclingConverterManagerConfig):
        self.config = config

        self.ocr_factory = get_ocr_factory(
            allow_external_plugins=self.config.allow_external_plugins
        )

        # Initialize factories for kind selection
        self.table_structure_factory = get_table_structure_factory(
            allow_external_plugins=self.config.allow_external_plugins
        )

        self.layout_factory = get_layout_factory(
            allow_external_plugins=self.config.allow_external_plugins
        )

        self._options_map: dict[bytes, PdfFormatOption] = {}
        self._get_converter_from_hash = self._create_converter_cache_from_hash(
            cache_size=self.config.options_cache_size
        )

        self._cache_lock = threading.Lock()

        # Build preset registries
        self._build_preset_registries()

        # Build kind registries
        self._build_kind_registries()

    def _create_converter_cache_from_hash(
        self, cache_size: int
    ) -> Callable[[bytes], DocumentConverter]:
        @lru_cache(maxsize=cache_size)
        def _get_converter_from_hash(options_hash: bytes) -> DocumentConverter:
            pdf_format_option = self._options_map[options_hash]
            image_format_option: FormatOption = pdf_format_option
            if isinstance(pdf_format_option.pipeline_cls, type) and issubclass(
                pdf_format_option.pipeline_cls, VlmPipeline
            ):
                image_format_option = ImageFormatOption(
                    pipeline_cls=pdf_format_option.pipeline_cls,
                    pipeline_options=pdf_format_option.pipeline_options,
                    backend_options=pdf_format_option.backend_options,
                )

            format_options: dict[InputFormat, FormatOption] = {
                InputFormat.PDF: pdf_format_option,
                InputFormat.IMAGE: image_format_option,
            }

            return DocumentConverter(format_options=format_options)

        return _get_converter_from_hash

    def clear_cache(self):
        self._get_converter_from_hash.cache_clear()

    def _build_preset_registries(self):
        """Build internal registries of allowed presets."""
        # VLM Pipeline Registry
        self.vlm_preset_registry: dict[str, VlmPresetInfo] = {}

        # ALWAYS add "default" preset (stable, guaranteed)
        self.vlm_preset_registry["default"] = {
            "source": "docling",
            "preset_id": self.config.default_vlm_preset,
        }

        # Add Docling built-in presets (if allowed)
        if self.config.allowed_vlm_presets is None:
            # Allow all Docling presets
            for preset_id in VlmConvertOptions.list_preset_ids():
                if preset_id != self.config.default_vlm_preset:
                    self.vlm_preset_registry[preset_id] = {
                        "source": "docling",
                        "preset_id": preset_id,
                    }
        else:
            # Only allow specified presets
            for preset_id in self.config.allowed_vlm_presets:
                if preset_id != self.config.default_vlm_preset:
                    self.vlm_preset_registry[preset_id] = {
                        "source": "docling",
                        "preset_id": preset_id,
                    }

        # Add custom presets (always allowed)
        for preset_id, preset_options in self.config.custom_vlm_presets.items():
            self.vlm_preset_registry[preset_id] = {
                "source": "custom",
                "options": preset_options,
            }

        # Picture Description Registry
        self.picture_description_preset_registry: dict[
            str, PictureDescriptionPresetInfo
        ] = {}

        self.picture_description_preset_registry["default"] = {
            "source": "docling",
            "preset_id": self.config.default_picture_description_preset,
        }

        # Add allowed presets if specified
        if self.config.allowed_picture_description_presets is not None:
            for preset_id in self.config.allowed_picture_description_presets:
                if preset_id != "default":
                    self.picture_description_preset_registry[preset_id] = {
                        "source": "docling",
                        "preset_id": preset_id,
                    }

        # Add custom presets
        for (
            preset_id,
            preset_options,
        ) in self.config.custom_picture_description_presets.items():
            self.picture_description_preset_registry[preset_id] = {
                "source": "custom",
                "options": preset_options,
            }

        # Code/Formula Registry
        self.code_formula_preset_registry: dict[str, CodeFormulaPresetInfo] = {}

        self.code_formula_preset_registry["default"] = {
            "source": "docling",
            "preset_id": self.config.default_code_formula_preset,
        }

        # Add allowed presets if specified
        if self.config.allowed_code_formula_presets is not None:
            for preset_id in self.config.allowed_code_formula_presets:
                if preset_id != "default":
                    self.code_formula_preset_registry[preset_id] = {
                        "source": "docling",
                        "preset_id": preset_id,
                    }

        # Add custom presets
        for (
            preset_id,
            preset_options,
        ) in self.config.custom_code_formula_presets.items():
            self.code_formula_preset_registry[preset_id] = {
                "source": "custom",
                "options": preset_options,
            }

    def _build_kind_registries(self):
        """Build registries of available kinds from factories."""
        # Table Structure Kinds
        self.available_table_structure_kinds = (
            self.table_structure_factory.registered_kind
        )

        # Layout Kinds
        self.available_layout_kinds = self.layout_factory.registered_kind

    def _validate_kind_available(
        self, kind: str, available_kinds: list[str], stage_name: str
    ) -> None:
        """Validate that a kind is available from the factory."""
        if kind not in available_kinds:
            raise ValueError(
                f"{stage_name} kind '{kind}' is not available. "
                f"Available kinds: {', '.join(available_kinds)}. "
                f"If this is a plugin, ensure it is properly installed."
            )

    def _validate_kind_allowed(
        self,
        kind: str,
        allowed_kinds: Optional[list[str]],
        default_kind: str,
        stage_name: str,
    ) -> None:
        """Validate that a kind is allowed by admin configuration (pre-filter)."""
        # Default kind is always allowed
        if kind == default_kind:
            return

        # If no allowlist, all kinds are allowed
        if allowed_kinds is None:
            return

        # Check if kind is in allowlist
        if kind not in allowed_kinds:
            raise ValueError(
                f"{stage_name} kind '{kind}' is not allowed by administrator. "
                f"Allowed kinds: {', '.join([default_kind, *allowed_kinds])}. "
                f"Contact your administrator to enable this kind."
            )

    def _validate_preset(
        self,
        preset_id: str,
        registry: Mapping[str, AnyPresetInfo],
        preset_type: str,
    ) -> None:
        """Generic preset validation."""
        if preset_id not in registry:
            allowed = list(registry.keys())
            raise ValueError(
                f"{preset_type} preset '{preset_id}' is not allowed. "
                f"Allowed presets: {', '.join(allowed)}"
            )

    def _validate_custom_config_allowed(self, config_type: str) -> None:
        """Validate that custom configuration is allowed."""
        if config_type == "vlm" and not self.config.allow_custom_vlm_config:
            raise ValueError(
                "Custom VLM configuration is not allowed. "
                "Please use a preset or contact your administrator."
            )
        elif (
            config_type == "picture_description"
            and not self.config.allow_custom_picture_description_config
        ):
            raise ValueError(
                "Custom picture description configuration is not allowed. "
                "Please use a preset or contact your administrator."
            )
        elif (
            config_type == "code_formula"
            and not self.config.allow_custom_code_formula_config
        ):
            raise ValueError(
                "Custom code/formula configuration is not allowed. "
                "Please use a preset or contact your administrator."
            )

    def _validate_engine_allowed(
        self, engine_type: str, allowed_engines: Optional[list[str]]
    ) -> None:
        """Validate that engine type is allowed."""
        if allowed_engines is not None and engine_type not in allowed_engines:
            raise ValueError(
                f"Engine '{engine_type}' is not allowed. "
                f"Allowed engines: {', '.join(allowed_engines)}"
            )

    def _get_options_from_preset(
        self,
        preset_id: str,
        registry: Mapping[str, AnyPresetInfo],
        preset_type: str,
        allowed_engines: Optional[list[str]],
        from_preset_func: Optional[Callable[[str], Any]] = None,
    ) -> Any:
        """Generic method to get options from preset with engine validation.

        Args:
            preset_id: The preset identifier
            registry: The preset registry to look up
            preset_type: Human-readable type name for error messages
            allowed_engines: List of allowed engine types (None means all allowed)
            from_preset_func: Optional function to create options from preset_id (e.g., VlmConvertOptions.from_preset)

        Returns:
            Options object or preset_id string (for legacy handling)
        """
        # Validate preset is allowed
        self._validate_preset(preset_id, registry, preset_type)

        preset_info = registry[preset_id]

        if preset_info["source"] == "docling":
            # Use Docling built-in preset
            if from_preset_func is not None:
                options = from_preset_func(preset_info["preset_id"])

                # Validate engine is allowed
                if allowed_engines is not None and hasattr(options, "engine_options"):
                    engine_type = getattr(options.engine_options, "engine_type")
                    self._validate_engine_allowed(engine_type, allowed_engines)

                return options
            else:
                # Return preset_id for legacy handling
                # TODO: Update when all options have from_preset method
                return preset_info["preset_id"]
        else:  # custom preset
            # Use admin-configured preset
            preset_options = preset_info["options"]

            # Validate engine is allowed (only for options that have engine_options)
            if allowed_engines is not None and hasattr(
                preset_options, "engine_options"
            ):
                engine_options = getattr(preset_options, "engine_options")
                engine_type = getattr(engine_options, "engine_type")
                self._validate_engine_allowed(engine_type, allowed_engines)

            return preset_options

    def _instantiate_engine_options(
        self, engine_options_dict: dict
    ) -> BaseVlmEngineOptions:
        """Instantiate the correct engine options class based on engine_type.

        Pydantic doesn't automatically discriminate BaseVlmEngineOptions subclasses,
        so we need to manually instantiate the correct class based on engine_type.
        """
        engine_type = engine_options_dict.get("engine_type")

        if not engine_type:
            raise ValueError("engine_options must contain 'engine_type' field")

        # Map engine_type to the correct class
        engine_class_map: dict[VlmEngineType, Type[BaseVlmEngineOptions]] = {
            VlmEngineType.MLX: MlxVlmEngineOptions,
            VlmEngineType.TRANSFORMERS: TransformersVlmEngineOptions,
            VlmEngineType.VLLM: VllmVlmEngineOptions,
            VlmEngineType.API: ApiVlmEngineOptions,
            VlmEngineType.API_OLLAMA: ApiVlmEngineOptions,
            VlmEngineType.API_LMSTUDIO: ApiVlmEngineOptions,
            VlmEngineType.API_OPENAI: ApiVlmEngineOptions,
            VlmEngineType.AUTO_INLINE: AutoInlineVlmEngineOptions,
        }

        # Handle string engine_type (convert to enum if needed)
        if isinstance(engine_type, str):
            try:
                engine_type = VlmEngineType(engine_type)
            except ValueError:
                raise ValueError(f"Invalid engine_type: {engine_type}")

        engine_class = engine_class_map.get(engine_type)
        if not engine_class:
            raise ValueError(f"Unsupported engine_type: {engine_type}")

        return engine_class.model_validate(engine_options_dict)

    def _parse_vlm_options(
        self, request: ConvertDocumentsOptions
    ) -> Optional[VlmConvertOptions]:
        """Parse VLM options from preset OR custom config."""
        # Option 1: Preset (recommended)
        if request.vlm_pipeline_preset:
            return self._get_options_from_preset(
                request.vlm_pipeline_preset,
                self.vlm_preset_registry,
                "VLM",
                self.config.allowed_vlm_engines,
                VlmConvertOptions.from_preset,
            )

        # Option 2: Custom config (if allowed)
        if request.vlm_pipeline_custom_config:
            self._validate_custom_config_allowed("vlm")

            # If it's already a VlmConvertOptions object, validate and return
            if isinstance(request.vlm_pipeline_custom_config, VlmConvertOptions):
                # Validate engine is allowed
                if self.config.allowed_vlm_engines is not None:
                    engine_type = (
                        request.vlm_pipeline_custom_config.engine_options.engine_type
                    )
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_vlm_engines
                    )
                return request.vlm_pipeline_custom_config

            # If it's a dict, convert it to VlmConvertOptions
            if isinstance(request.vlm_pipeline_custom_config, dict):
                config_dict = request.vlm_pipeline_custom_config.copy()

                # Validate engine is allowed
                if self.config.allowed_vlm_engines is not None:
                    engine_options_dict = config_dict.get("engine_options", {})
                    engine_type = engine_options_dict.get("engine_type", "")
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_vlm_engines
                    )

                # Instantiate the correct engine options class
                if "engine_options" in config_dict and isinstance(
                    config_dict["engine_options"], dict
                ):
                    config_dict["engine_options"] = self._instantiate_engine_options(
                        config_dict["engine_options"]
                    )

                # Convert dict to VlmConvertOptions
                return VlmConvertOptions.model_validate(config_dict)

            # Should not reach here, but handle gracefully
            raise ValueError(
                f"Invalid vlm_pipeline_custom_config type: {type(request.vlm_pipeline_custom_config)}"
            )

        # Option 3: No new-style config specified
        return None

    def _parse_picture_description_options(
        self, request: ConvertDocumentsOptions
    ) -> Optional[Any]:
        """Parse picture description options from preset OR custom config."""
        if request.picture_description_preset:
            return self._get_options_from_preset(
                request.picture_description_preset,
                self.picture_description_preset_registry,
                "Picture description",
                self.config.allowed_picture_description_engines,
                None,  # No from_preset method yet
            )

        if request.picture_description_custom_config:
            self._validate_custom_config_allowed("picture_description")

            # If it's already a PictureDescriptionVlmEngineOptions object, validate and return
            from docling.datamodel.pipeline_options import (
                PictureDescriptionVlmEngineOptions,
            )

            if isinstance(
                request.picture_description_custom_config,
                PictureDescriptionVlmEngineOptions,
            ):
                if self.config.allowed_picture_description_engines is not None:
                    engine_type = request.picture_description_custom_config.engine_options.engine_type
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_picture_description_engines
                    )
                return request.picture_description_custom_config

            # If it's a dict, convert it to PictureDescriptionVlmEngineOptions
            if isinstance(request.picture_description_custom_config, dict):
                config_dict = request.picture_description_custom_config.copy()

                if self.config.allowed_picture_description_engines is not None:
                    engine_options_dict = config_dict.get("engine_options", {})
                    engine_type = engine_options_dict.get("engine_type", "")
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_picture_description_engines
                    )

                # Instantiate the correct engine options class
                if "engine_options" in config_dict and isinstance(
                    config_dict["engine_options"], dict
                ):
                    config_dict["engine_options"] = self._instantiate_engine_options(
                        config_dict["engine_options"]
                    )

                return PictureDescriptionVlmEngineOptions.model_validate(config_dict)

            raise ValueError(
                f"Invalid picture_description_custom_config type: {type(request.picture_description_custom_config)}"
            )

        return None

    def _parse_code_formula_options(
        self, request: ConvertDocumentsOptions
    ) -> Optional[Any]:
        """Parse code/formula options from preset OR custom config."""
        if request.code_formula_preset:
            return self._get_options_from_preset(
                request.code_formula_preset,
                self.code_formula_preset_registry,
                "Code/formula",
                self.config.allowed_code_formula_engines,
                None,  # No from_preset method yet
            )

        if request.code_formula_custom_config:
            self._validate_custom_config_allowed("code_formula")

            # If it's already a CodeFormulaVlmOptions object, validate and return
            from docling.datamodel.pipeline_options import CodeFormulaVlmOptions

            if isinstance(request.code_formula_custom_config, CodeFormulaVlmOptions):
                if self.config.allowed_code_formula_engines is not None:
                    engine_type = (
                        request.code_formula_custom_config.engine_options.engine_type
                    )
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_code_formula_engines
                    )
                return request.code_formula_custom_config

            # If it's a dict, convert it to CodeFormulaVlmOptions
            if isinstance(request.code_formula_custom_config, dict):
                config_dict = request.code_formula_custom_config.copy()

                if self.config.allowed_code_formula_engines is not None:
                    engine_options_dict = config_dict.get("engine_options", {})
                    engine_type = engine_options_dict.get("engine_type", "")
                    self._validate_engine_allowed(
                        engine_type, self.config.allowed_code_formula_engines
                    )

                # Instantiate the correct engine options class
                if "engine_options" in config_dict and isinstance(
                    config_dict["engine_options"], dict
                ):
                    config_dict["engine_options"] = self._instantiate_engine_options(
                        config_dict["engine_options"]
                    )

                return CodeFormulaVlmOptions.model_validate(config_dict)

            raise ValueError(
                f"Invalid code_formula_custom_config type: {type(request.code_formula_custom_config)}"
            )

        return None

    def _parse_table_structure_options(self, request: ConvertDocumentsOptions) -> Any:
        """Parse table structure options - preset for default kind, custom config for others."""

        if request.table_structure_custom_config:
            # Custom config provided - validate and use it
            config_dict = request.table_structure_custom_config.copy()
            kind = config_dict.get("kind")

            if not kind:
                raise ValueError(
                    "table_structure_custom_config must include a 'kind' field "
                    "specifying which table structure implementation to use."
                )

            # Validate kind is allowed (pre-filter before factory)
            self._validate_kind_allowed(
                kind,
                self.config.allowed_table_structure_kinds,
                self.config.default_table_structure_kind,
                "table structure",
            )

            # Validate kind is available from factory
            self._validate_kind_available(
                kind, self.available_table_structure_kinds, "table structure"
            )

            # Create options using factory with custom config
            return self.table_structure_factory.create_options(
                kind=kind, **{k: v for k, v in config_dict.items() if k != "kind"}
            )

        else:
            # Use default kind with legacy fields (table_mode, table_cell_matching)
            kind = self.config.default_table_structure_kind
            return self.table_structure_factory.create_options(
                kind=kind,
                mode=TableFormerMode(request.table_mode),
                do_cell_matching=request.table_cell_matching,
            )

    def _parse_layout_options(self, request: ConvertDocumentsOptions) -> Any:
        """Parse layout options - preset for default kind, custom config for others."""

        if request.layout_custom_config:
            # Custom config provided - validate and use it
            config_dict = request.layout_custom_config.copy()
            kind = config_dict.get("kind")

            if not kind:
                raise ValueError(
                    "layout_custom_config must include a 'kind' field "
                    "specifying which layout implementation to use."
                )

            # Validate kind is allowed (pre-filter before factory)
            self._validate_kind_allowed(
                kind,
                self.config.allowed_layout_kinds,
                self.config.default_layout_kind,
                "layout",
            )

            # Validate kind is available from factory
            self._validate_kind_available(kind, self.available_layout_kinds, "layout")

            # Create options using factory with custom config
            return self.layout_factory.create_options(
                kind=kind, **{k: v for k, v in config_dict.items() if k != "kind"}
            )

        else:
            # Use default kind with preset/default configuration
            kind = self.config.default_layout_kind
            return self.layout_factory.create_options(kind=kind)

    def get_converter(self, pdf_format_option: PdfFormatOption) -> DocumentConverter:
        with self._cache_lock:
            options_hash = _hash_pdf_format_option(pdf_format_option)
            self._options_map[options_hash] = pdf_format_option
            converter = self._get_converter_from_hash(options_hash)
        return converter

    def _parse_standard_pdf_opts(
        self, request: ConvertDocumentsOptions, artifacts_path: Optional[Path]
    ) -> PdfPipelineOptions:
        try:
            kind = (
                request.ocr_engine.value
                if isinstance(request.ocr_engine, enum.Enum)
                else str(request.ocr_engine)
            )
            ocr_options: OcrOptions = self.ocr_factory.create_options(  # type: ignore
                kind=kind,
                force_full_page_ocr=request.force_ocr,
            )
        except ImportError as err:
            raise ImportError(
                "The requested OCR engine"
                f" (ocr_engine={request.ocr_engine})"
                " is not available on this system. Please choose another OCR engine "
                "or contact your system administrator.\n"
                f"{err}"
            )

        if request.ocr_lang is not None:
            ocr_options.lang = request.ocr_lang

        pipeline_options = PdfPipelineOptions(
            artifacts_path=artifacts_path,
            allow_external_plugins=self.config.allow_external_plugins,
            enable_remote_services=self.config.enable_remote_services,
            document_timeout=request.document_timeout,
            do_ocr=request.do_ocr,
            ocr_options=ocr_options,
            do_table_structure=request.do_table_structure,
            do_code_enrichment=request.do_code_enrichment,
            do_formula_enrichment=request.do_formula_enrichment,
            do_picture_classification=request.do_picture_classification,
            do_chart_extraction=request.do_chart_extraction,
            do_picture_description=request.do_picture_description,
        )
        # === NEW KIND-BASED APPROACH for Table Structure ===
        # Try new custom config approach first, fall back to legacy fields
        if request.table_structure_custom_config:
            pipeline_options.table_structure_options = (
                self._parse_table_structure_options(request)
            )
        else:
            # Legacy approach - use table_mode and table_cell_matching fields
            pipeline_options.table_structure_options = TableStructureOptions(
                mode=TableFormerMode(request.table_mode),
                do_cell_matching=request.table_cell_matching,
            )

        # === NEW KIND-BASED APPROACH for Layout ===
        # Try new custom config approach first
        if request.layout_custom_config:
            pipeline_options.layout_options = self._parse_layout_options(request)
        # If no custom config, use default (factory will handle it)
        # Note: Layout options are optional, so we don't set them if not specified

        if request.image_export_mode != ImageRefMode.PLACEHOLDER:
            pipeline_options.generate_page_images = True
            if request.image_export_mode == ImageRefMode.REFERENCED:
                pipeline_options.generate_picture_images = True
            if request.images_scale:
                pipeline_options.images_scale = request.images_scale

        # === NEW ENGINE-BASED APPROACH for Picture Description ===
        new_picture_desc_options = self._parse_picture_description_options(request)
        if new_picture_desc_options is not None:
            # New-style configuration provided
            if isinstance(new_picture_desc_options, dict):
                pipeline_options.picture_description_options = (
                    PictureDescriptionVlmOptions.model_validate(
                        new_picture_desc_options
                    )
                )
            elif isinstance(new_picture_desc_options, str):
                # Preset ID - for now, keep legacy behavior
                # TODO: Update when PictureDescriptionVlmOptions has from_preset
                pass
            else:
                # Already an options object
                pipeline_options.picture_description_options = new_picture_desc_options
        else:
            # === LEGACY APPROACH for Picture Description ===
            if request.picture_description_local is not None:
                pipeline_options.picture_description_options = (
                    PictureDescriptionVlmOptions.model_validate(
                        request.picture_description_local.model_dump()
                    )
                )

            if request.picture_description_api is not None:
                pipeline_options.picture_description_options = (
                    PictureDescriptionApiOptions.model_validate(
                        request.picture_description_api.model_dump()
                    )
                )

        # Set picture area threshold (common to both approaches)
        pipeline_options.picture_description_options.picture_area_threshold = (
            request.picture_description_area_threshold
        )

        # Forward the definition of the following attributes, if they are not none
        for attr in (
            "queue_max_size",
            "ocr_batch_size",
            "layout_batch_size",
            "table_batch_size",
            "batch_polling_interval_seconds",
        ):
            if value := getattr(self.config, attr):
                setattr(pipeline_options, attr, value)

        return pipeline_options

    def _parse_backend(
        self, request: ConvertDocumentsOptions
    ) -> type[PdfDocumentBackend]:
        pdf_backend = normalize_pdf_backend(request.pdf_backend)
        if pdf_backend == PdfBackend.DOCLING_PARSE:
            backend: type[PdfDocumentBackend] = DoclingParseDocumentBackend
        elif pdf_backend == PdfBackend.PYPDFIUM2:
            backend = PyPdfiumDocumentBackend
        else:
            raise RuntimeError(f"Unexpected PDF backend type {request.pdf_backend}")

        return backend

    def _parse_vlm_pdf_opts(
        self, request: ConvertDocumentsOptions, artifacts_path: Optional[Path]
    ) -> VlmPipelineOptions:
        pipeline_options = VlmPipelineOptions(
            artifacts_path=artifacts_path,
            document_timeout=request.document_timeout,
            enable_remote_services=self.config.enable_remote_services,
        )

        # === NEW ENGINE-BASED APPROACH ===
        # Try new preset/custom config approach first
        new_vlm_options = self._parse_vlm_options(request)
        if new_vlm_options is not None:
            # New-style configuration provided
            if isinstance(new_vlm_options, dict):
                # Convert dict to VlmConvertOptions
                pipeline_options.vlm_options = VlmConvertOptions.model_validate(
                    new_vlm_options
                )
            else:
                # Already a VlmConvertOptions object
                pipeline_options.vlm_options = new_vlm_options
        else:
            # === LEGACY APPROACH (Backwards Compatibility) ===
            # Fall back to legacy configuration
            if request.vlm_pipeline_model in (
                None,
                vlm_model_specs.VlmModelType.GRANITEDOCLING,
            ):
                pipeline_options.vlm_options = (
                    vlm_model_specs.GRANITEDOCLING_TRANSFORMERS
                )
                if sys.platform == "darwin":
                    try:
                        import mlx_vlm  # noqa: F401

                        pipeline_options.vlm_options = (
                            vlm_model_specs.GRANITEDOCLING_MLX
                        )
                    except ImportError:
                        _log.warning(
                            "To run GraniteDocling faster, please install mlx-vlm:\n"
                            "pip install mlx-vlm"
                        )

            elif (
                request.vlm_pipeline_model
                == vlm_model_specs.VlmModelType.GRANITE_VISION
            ):
                pipeline_options.vlm_options = (
                    vlm_model_specs.GRANITE_VISION_TRANSFORMERS
                )

            elif (
                request.vlm_pipeline_model
                == vlm_model_specs.VlmModelType.GRANITE_VISION_OLLAMA
            ):
                pipeline_options.vlm_options = vlm_model_specs.GRANITE_VISION_OLLAMA

            if request.vlm_pipeline_model_local is not None:
                pipeline_options.vlm_options = InlineVlmOptions.model_validate(
                    request.vlm_pipeline_model_local.model_dump()
                )

            if request.vlm_pipeline_model_api is not None:
                pipeline_options.vlm_options = ApiVlmOptions.model_validate(
                    request.vlm_pipeline_model_api.model_dump()
                )

        # Picture classification and description (common to both approaches)
        pipeline_options.do_picture_classification = request.do_picture_classification
        pipeline_options.do_picture_description = request.do_picture_description

        # === NEW ENGINE-BASED APPROACH for Picture Description ===
        new_picture_desc_options = self._parse_picture_description_options(request)
        if new_picture_desc_options is not None:
            # New-style configuration provided
            if isinstance(new_picture_desc_options, dict):
                pipeline_options.picture_description_options = (
                    PictureDescriptionVlmOptions.model_validate(
                        new_picture_desc_options
                    )
                )
            elif isinstance(new_picture_desc_options, str):
                # Preset ID - for now, keep legacy behavior
                # TODO: Update when PictureDescriptionVlmOptions has from_preset
                pass
            else:
                # Already an options object
                pipeline_options.picture_description_options = new_picture_desc_options
        else:
            # === LEGACY APPROACH for Picture Description ===
            if request.picture_description_local is not None:
                pipeline_options.picture_description_options = (
                    PictureDescriptionVlmOptions.model_validate(
                        request.picture_description_local.model_dump()
                    )
                )

            if request.picture_description_api is not None:
                pipeline_options.picture_description_options = (
                    PictureDescriptionApiOptions.model_validate(
                        request.picture_description_api.model_dump()
                    )
                )

        # Set picture area threshold (common to both approaches)
        pipeline_options.picture_description_options.picture_area_threshold = (
            request.picture_description_area_threshold
        )

        return pipeline_options

    # Computes the PDF pipeline options and returns the PdfFormatOption and its hash
    def get_pdf_pipeline_opts(
        self,
        request: ConvertDocumentsOptions,
    ) -> PdfFormatOption:
        artifacts_path: Optional[Path] = None
        if self.config.artifacts_path is not None:
            expanded_path = self.config.artifacts_path.expanduser()
            if str(expanded_path.absolute()) == "":
                _log.info(
                    "artifacts_path is an empty path, model weights will be downloaded "
                    "at runtime."
                )
                artifacts_path = None
            elif expanded_path.is_dir():
                _log.info(
                    "artifacts_path is set to a valid directory. "
                    "No model weights will be downloaded at runtime."
                )
                artifacts_path = expanded_path
            else:
                _log.warning(
                    "artifacts_path is set to an invalid directory. "
                    "The system will download the model weights at runtime."
                )
                artifacts_path = None
        else:
            _log.info(
                "artifacts_path is unset. "
                "The system will download the model weights at runtime."
            )

        pipeline_options: Union[PdfPipelineOptions, VlmPipelineOptions]
        if request.pipeline == ProcessingPipeline.STANDARD:
            pipeline_options = self._parse_standard_pdf_opts(request, artifacts_path)
            backend = self._parse_backend(request)
            pdf_format_option = PdfFormatOption(
                pipeline_options=pipeline_options,
                backend=backend,
            )

        elif request.pipeline == ProcessingPipeline.VLM:
            pipeline_options = self._parse_vlm_pdf_opts(request, artifacts_path)
            pdf_format_option = PdfFormatOption(
                pipeline_cls=VlmPipeline, pipeline_options=pipeline_options
            )
        else:
            raise NotImplementedError(
                f"The pipeline {request.pipeline} is not implemented."
            )

        return pdf_format_option

    def convert_documents(
        self,
        sources: Iterable[Union[Path, str, DocumentStream]],
        options: ConvertDocumentsOptions,
        headers: Optional[dict[str, Any]] = None,
    ) -> Iterable[ConversionResult]:
        pdf_format_option = self.get_pdf_pipeline_opts(options)
        converter = self.get_converter(pdf_format_option)
        with self._cache_lock:
            converter.initialize_pipeline(format=InputFormat.PDF)

        results: Iterator[ConversionResult] = converter.convert_all(
            sources,
            headers=headers,
            page_range=options.page_range,
            max_file_size=self.config.max_file_size,
            max_num_pages=self.config.max_num_pages,
            raises_on_error=options.abort_on_error,
        )

        return results
