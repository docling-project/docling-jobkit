"""Thin wrapper around docling's ``DocumentExtractor`` facade.

Unlike ``DoclingConverterManager`` this does not rebuild any pipeline-options
machinery — docling owns the extraction model registry and the per-format
backend defaults. The manager's only real job is the **operator-gating
boundary**: which extraction model a request is allowed to run against
(mirrors ``allow_custom_vlm_config`` / ``allow_custom_ocr_config`` in
``manager.py`` and docling's ``{stage}_preset`` + ``{stage}_custom_config``
convention).
"""

import logging
import sys
import threading
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel

from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.datamodel.extraction import ExtractionResult
from docling.datamodel.extraction_options import (
    ChannelSelection,
    ExtractionVlmOptions,
)
from docling.datamodel.pipeline_options import VlmExtractionPipelineOptions
from docling.datamodel.service.options import ExtractDocumentsOptions
from docling.document_extractor import (
    DEFAULT_EXTRACTION_FORMATS,
    DocumentExtractor,
    ExtractionFormatOption,
)
from docling.models.inference_engines.vlm.base import VlmEngineType
from docling.pipeline.extraction_vlm_pipeline import ExtractionVlmPipeline

_log = logging.getLogger(__name__)


class DocumentExtractionManagerConfig(BaseModel):
    artifacts_path: Optional[Path] = None
    options_cache_size: int = 2
    enable_remote_services: bool = False
    allow_external_plugins: bool = False

    max_num_pages: int = sys.maxsize
    max_file_size: int = sys.maxsize

    # None -> serve the full docling default set (IMAGE/PDF/DOCX/HTML/MD/DCLX).
    allowed_formats: Optional[list[InputFormat]] = None

    # Operator model-selection gating (the security boundary).
    default_extraction_preset: str = "nuextract_2b"
    # Subset of ExtractionVlmOptions.list_preset_ids(); None allows any registered id.
    allowed_extraction_presets: Optional[list[str]] = None
    allow_custom_extraction_config: bool = False
    allowed_extraction_engines: Optional[list[str]] = None


class DocumentExtractionManager:
    def __init__(self, config: DocumentExtractionManagerConfig):
        self.config = config
        self._cache_lock = threading.Lock()
        self._get_extractor = self._create_extractor_cache(config.options_cache_size)

    def _create_extractor_cache(self, cache_size: int):
        @lru_cache(maxsize=cache_size)
        def _get_extractor(
            pipeline_options_json: str, formats: tuple[InputFormat, ...]
        ) -> DocumentExtractor:
            pipe_opts = VlmExtractionPipelineOptions.model_validate_json(
                pipeline_options_json
            )
            return DocumentExtractor(
                allowed_formats=list(formats),
                extraction_format_options={
                    fmt: ExtractionFormatOption(
                        pipeline_cls=ExtractionVlmPipeline,
                        pipeline_options=pipe_opts,
                    )
                    for fmt in formats
                },
            )

        return _get_extractor

    def _resolve_preset(self, preset_id: str) -> ExtractionVlmOptions:
        """Resolve a preset id through docling's registry, allow-list first."""
        allowed = self.config.allowed_extraction_presets
        if allowed is not None and preset_id not in allowed:
            raise ValueError(
                f"Extraction preset {preset_id!r} is not allowed. "
                f"Allowed presets: {', '.join(allowed)}"
            )
        try:
            return ExtractionVlmOptions.from_preset(preset_id)
        except KeyError as exc:
            # Surface docling's "available presets" message as a request error.
            raise ValueError(str(exc)) from exc

    def resolve_extraction_model(
        self, options: ExtractDocumentsOptions
    ) -> ExtractionVlmOptions:
        """Operator-gated model selection (mirrors convert's custom-config gate)."""
        if options.extraction_custom_config is not None:
            if not self.config.allow_custom_extraction_config:
                raise ValueError(
                    "Custom extraction configuration is not allowed. "
                    "Please use a preset or contact your administrator."
                )
            custom = options.extraction_custom_config
            resolved = (
                custom
                if isinstance(custom, ExtractionVlmOptions)
                else ExtractionVlmOptions.model_validate(custom)
            )
        elif options.extraction_preset:
            resolved = self._resolve_preset(options.extraction_preset)
        else:
            resolved = self._resolve_preset(self.config.default_extraction_preset)

        engine = resolved.engine_options.engine_type
        allowed_engines = self.config.allowed_extraction_engines
        if allowed_engines is not None and engine not in allowed_engines:
            raise ValueError(
                f"Extraction engine {engine!r} is not allowed. "
                f"Allowed engines: {', '.join(allowed_engines)}"
            )
        if (
            VlmEngineType.is_api_variant(engine)
            and not self.config.enable_remote_services
        ):
            raise ValueError(
                "Remote extraction services are disabled by server policy."
            )
        return resolved

    def extract_documents(
        self,
        sources: Iterable[Union[Path, str, DocumentStream]],
        options: ExtractDocumentsOptions,
        headers: Optional[dict[str, str]] = None,
    ) -> Iterable[ExtractionResult]:
        vlm_options = self.resolve_extraction_model(options)
        pipe_opts = VlmExtractionPipelineOptions(
            vlm_options=vlm_options,
            input_channels=options.input_channels or ChannelSelection.AUTO,
            enable_remote_services=self.config.enable_remote_services,
            artifacts_path=self.config.artifacts_path,
        )
        # Backends default per format inside DocumentExtractor.__init__ (leave
        # `backend` unset); we only override pipeline_options so every allowed
        # format shares the one resolved model.
        formats = tuple(self.config.allowed_formats or DEFAULT_EXTRACTION_FORMATS)
        with self._cache_lock:
            extractor = self._get_extractor(pipe_opts.model_dump_json(), formats)
        # raises_on_error=False: per-document failures surface as FAILURE status
        # on each ExtractionResult rather than aborting the whole task.
        return extractor.extract_all(
            sources,
            template=options.template,
            page_range=options.page_range,
            max_num_pages=self.config.max_num_pages,
            max_file_size=self.config.max_file_size,
            headers=headers,
            raises_on_error=False,
        )
