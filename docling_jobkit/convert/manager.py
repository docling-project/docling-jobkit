import enum
import hashlib
import json
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Optional, Union

from pydantic import BaseModel

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.backend.pdf_backend import PdfDocumentBackend
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel import vlm_model_specs
from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import (
    OcrOptions,
    PdfBackend,
    PdfPipelineOptions,
    PictureDescriptionApiOptions,
    PictureDescriptionVlmOptions,
    ProcessingPipeline,
    TableFormerMode,
    VlmPipelineOptions,
)
from docling.document_converter import DocumentConverter, FormatOption, PdfFormatOption
from docling.models.factories import get_ocr_factory
from docling.pipeline.vlm_pipeline import VlmPipeline
from docling_core.types.doc import ImageRefMode

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions

_log = logging.getLogger(__name__)


class DoclingConverterManagerConfig(BaseModel):
    artifacts_path: Optional[Path] = None
    options_cache_size: int = 2
    enable_remote_services: bool = False
    allow_external_plugins: bool = False

    max_num_pages: int = sys.maxsize
    max_file_size: int = sys.maxsize


# Custom serializer for PdfFormatOption
# (model_dump_json does not work with some classes)
def _hash_pdf_format_option(pdf_format_option: PdfFormatOption) -> bytes:
    data = pdf_format_option.model_dump(serialize_as_any=True)

    # pipeline_options are not fully serialized by model_dump, dedicated pass
    if pdf_format_option.pipeline_options:
        data["pipeline_options"] = pdf_format_option.pipeline_options.model_dump(
            serialize_as_any=True, mode="json"
        )

    # Replace `pipeline_cls` with a string representation
    data["pipeline_cls"] = repr(data["pipeline_cls"])

    # Replace `backend` with a string representation
    data["backend"] = repr(data["backend"])

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
        self._options_map: dict[bytes, PdfFormatOption] = {}
        self._get_converter_from_hash = self._create_converter_cache_from_hash(
            cache_size=self.config.options_cache_size
        )

    def _create_converter_cache_from_hash(
        self, cache_size: int
    ) -> Callable[[bytes], DocumentConverter]:
        @lru_cache(maxsize=cache_size)
        def _get_converter_from_hash(options_hash: bytes) -> DocumentConverter:
            pdf_format_option = self._options_map[options_hash]
            format_options: dict[InputFormat, FormatOption] = {
                InputFormat.PDF: pdf_format_option,
                InputFormat.IMAGE: pdf_format_option,
            }

            return DocumentConverter(format_options=format_options)

        return _get_converter_from_hash

    def clear_cache(self):
        self._get_converter_from_hash.cache_clear()

    def get_converter(self, pdf_format_option: PdfFormatOption) -> DocumentConverter:
        options_hash = _hash_pdf_format_option(pdf_format_option)
        self._options_map[options_hash] = pdf_format_option
        return self._get_converter_from_hash(options_hash)

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
            enable_remote_services=self.config.enable_remote_services,
            document_timeout=request.document_timeout,
            do_ocr=request.do_ocr,
            ocr_options=ocr_options,
            do_table_structure=request.do_table_structure,
            do_code_enrichment=request.do_code_enrichment,
            do_formula_enrichment=request.do_formula_enrichment,
            do_picture_classification=request.do_picture_classification,
            do_picture_description=request.do_picture_description,
        )
        pipeline_options.table_structure_options.mode = TableFormerMode(
            request.table_mode
        )

        if request.image_export_mode != ImageRefMode.PLACEHOLDER:
            pipeline_options.generate_page_images = True
            if request.image_export_mode == ImageRefMode.REFERENCED:
                pipeline_options.generate_picture_images = True
            if request.images_scale:
                pipeline_options.images_scale = request.images_scale

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
        pipeline_options.picture_description_options.picture_area_threshold = (
            request.picture_description_area_threshold
        )

        return pipeline_options

    def _parse_backend(
        self, request: ConvertDocumentsOptions
    ) -> type[PdfDocumentBackend]:
        if request.pdf_backend == PdfBackend.DLPARSE_V1:
            backend: type[PdfDocumentBackend] = DoclingParseDocumentBackend
        elif request.pdf_backend == PdfBackend.DLPARSE_V2:
            backend = DoclingParseV2DocumentBackend
        elif request.pdf_backend == PdfBackend.DLPARSE_V4:
            backend = DoclingParseV4DocumentBackend
        elif request.pdf_backend == PdfBackend.PYPDFIUM2:
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
        )
        pipeline_options.vlm_options = vlm_model_specs.SMOLDOCLING_TRANSFORMERS
        if sys.platform == "darwin":
            try:
                import mlx_vlm  # noqa: F401

                pipeline_options.vlm_options = vlm_model_specs.SMOLDOCLING_MLX
            except ImportError:
                _log.warning(
                    "To run SmolDocling faster, please install mlx-vlm:\n"
                    "pip install mlx-vlm"
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
        results: Iterator[ConversionResult] = converter.convert_all(
            sources,
            headers=headers,
            page_range=options.page_range,
            max_file_size=self.config.max_file_size,
            max_num_pages=self.config.max_num_pages,
        )

        return results
