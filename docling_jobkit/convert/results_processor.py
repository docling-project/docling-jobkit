import json
import logging
import os
import shutil
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from pandas import DataFrame

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.utils.utils import create_hash
from docling_core.types.doc.base import ImageRefMode
from docling_core.types.doc.document import (
    DocItem,
    DoclingDocument,
    PageItem,
    PictureClassificationData,
    PictureItem,
)
from docling_core.types.doc.labels import DocItemLabel

from docling_jobkit.connectors.s3_helper import (
    MAX_PARQUET_FILE_SIZE,
    classifier_labels,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor


class ResultsProcessor:
    def __init__(
        self,
        target_processor: BaseTargetProcessor,
        to_formats: list[str] | None = None,
        generate_page_images: bool = False,
        generate_picture_images: bool = False,
        export_parquet_file: bool = False,
        scratch_dir: Path | None = None,
    ):
        self._target_processor = target_processor

        self.export_page_images = generate_page_images
        self.export_images = generate_picture_images

        self.to_formats = to_formats
        self.export_parquet_file = export_parquet_file

        self.scratch_dir = scratch_dir or Path(tempfile.mkdtemp(prefix="docling_"))
        self.scratch_dir.mkdir(exist_ok=True, parents=True)

    def __del__(self):
        if self.scratch_dir is not None:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

    def process_documents(self, results: Iterable[ConversionResult]):
        pd_d = DataFrame()  # DataFrame to append parquet info
        try:
            for i, conv_res in enumerate(results):
                with tempfile.TemporaryDirectory(dir=self.scratch_dir) as tmpdirname:
                    temp_dir = Path(tmpdirname)
                    if conv_res.status == ConversionStatus.SUCCESS:
                        doc_hash = conv_res.input.document_hash
                        name_without_ext = os.path.splitext(conv_res.input.file)[0]
                        logging.debug(f"Converted {doc_hash} now saving results")

                        if os.path.exists(conv_res.input.file):
                            self._target_processor.upload_file(
                                filename=Path(conv_res.input.file),
                                target_filename=f"pdf/{name_without_ext}.pdf",
                                content_type="application/pdf",
                            )

                        if self.export_page_images:
                            # Export pages images:
                            self.upload_page_images(
                                conv_res.document.pages,
                                conv_res.input.document_hash,
                            )

                        if self.export_images:
                            # Export pictures
                            self.upload_pictures(
                                conv_res.document,
                                conv_res.input.document_hash,
                            )

                        if self.to_formats is None or (
                            self.to_formats and "json" in self.to_formats
                        ):
                            # Export Docling document format to JSON:
                            target_key = f"json/{name_without_ext}.json"
                            temp_json_file = temp_dir / f"{name_without_ext}.json"

                            conv_res.document.save_as_json(
                                filename=temp_json_file,
                                image_mode=ImageRefMode.REFERENCED,
                            )
                            self._target_processor.upload_file(
                                filename=temp_json_file,
                                target_filename=target_key,
                                content_type="application/json",
                            )
                        if self.to_formats is None or (
                            self.to_formats and "doctags" in self.to_formats
                        ):
                            # Export Docling document format to doctags:
                            target_key = f"doctags/{name_without_ext}.doctags.txt"

                            data = conv_res.document.export_to_doctags()
                            self._target_processor.upload_object(
                                obj=data,
                                target_filename=target_key,
                                content_type="text/plain",
                            )
                        if self.to_formats is None or (
                            self.to_formats and "md" in self.to_formats
                        ):
                            # Export Docling document format to markdown:
                            target_key = f"md/{name_without_ext}.md"

                            data = conv_res.document.export_to_markdown()
                            self._target_processor.upload_object(
                                obj=data,
                                target_filename=target_key,
                                content_type="text/markdown",
                            )
                        if self.to_formats is None or (
                            self.to_formats and "html" in self.to_formats
                        ):
                            # Export Docling document format to html:
                            target_key = f"html/{name_without_ext}.html"
                            temp_html_file = temp_dir / f"{name_without_ext}.html"

                            conv_res.document.save_as_html(temp_html_file)
                            self._target_processor.upload_file(
                                filename=temp_html_file,
                                target_filename=target_key,
                                content_type="text/html",
                            )

                        if self.to_formats is None or (
                            self.to_formats and "text" in self.to_formats
                        ):
                            # Export Docling document format to text:
                            target_key = f"txt/{name_without_ext}.txt"

                            data = conv_res.document.export_to_text()
                            self._target_processor.upload_object(
                                obj=data,
                                target_filename=target_key,
                                content_type="text/plain",
                            )
                        if self.export_parquet_file:
                            logging.info("saving document info in dataframe...")
                            # Save Docling parquet info into DataFrame:
                            pd_d = self.document_to_dataframe(
                                conv_res=conv_res,
                                pd_dataframe=pd_d,
                                filename=name_without_ext,
                            )

                        yield f"{doc_hash} - SUCCESS"

                    elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
                        yield f"{conv_res.input.file} - PARTIAL_SUCCESS"
                    else:
                        yield f"{conv_res.input.file} - FAILURE"

        finally:
            if self.export_parquet_file and not pd_d.empty:
                self.upload_parquet_file(pd_d)

    def upload_page_images(
        self,
        pages: dict[int, PageItem],
        doc_hash: str,
    ):
        for page_no, page in pages.items():
            try:
                if page.image and page.image.pil_image:
                    page_hash = create_hash(f"{doc_hash}_page_no_{page_no}")
                    page_dpi = page.image.dpi
                    page_path_suffix = f"pages/{page_hash}_{page_dpi}.png"
                    buf = BytesIO()
                    page.image.pil_image.save(buf, format="PNG")
                    buf.seek(0)
                    self._target_processor.upload_object(
                        obj=buf,
                        target_filename=page_path_suffix,
                        content_type="application/png",
                    )
                    page.image.uri = Path(".." + page_path_suffix)

            except Exception as exc:
                logging.error(
                    "Upload image of page with hash %r raised error: %r",
                    page_hash,
                    exc,
                )

    def upload_pictures(
        self,
        document: DoclingDocument,
        doc_hash: str,
    ):
        picture_number = 0
        for element, _level in document.iterate_items():
            if isinstance(element, PictureItem):
                if element.image and element.image.pil_image:
                    try:
                        element_hash = create_hash(f"{doc_hash}_img_{picture_number}")
                        element_dpi = element.image.dpi
                        element_path_suffix = f"images/{element_hash}_{element_dpi}.png"
                        buf = BytesIO()
                        element.image.pil_image.save(buf, format="PNG")
                        buf.seek(0)
                        self._target_processor.upload_object(
                            obj=buf,
                            target_filename=element_path_suffix,
                            content_type="application/png",
                        )
                        element.image.uri = Path(".." + element_path_suffix)

                    except Exception as exc:
                        logging.error(
                            "Upload picture with hash %r raised error: %r",
                            element_hash,
                            exc,
                        )
                    picture_number += 1

    def document_to_dataframe(
        self, conv_res: ConversionResult, pd_dataframe: DataFrame, filename: str
    ) -> DataFrame:
        result_table: list[dict[str, Any]] = []

        page_images = []
        for page_no, page in conv_res.document.pages.items():
            if page.image is not None and page.image.pil_image is not None:
                page_images.append(page.image.pil_image.tobytes())

        # Count the number of picture of each type
        num_formulas = 0
        num_codes = 0
        picture_classes = dict.fromkeys(classifier_labels, 0)
        for element, _level in conv_res.document.iterate_items():
            if isinstance(element, PictureItem):
                element.image = None  # reset images
                classification = next(
                    (
                        annot
                        for annot in element.annotations
                        if isinstance(annot, PictureClassificationData)
                    ),
                    None,
                )
                if classification is None or len(classification.predicted_classes) == 0:
                    continue

                predicted_class = classification.predicted_classes[0].class_name
                if predicted_class in picture_classes:
                    picture_classes[predicted_class] += 1

            elif isinstance(element, DocItem):
                if element.label == DocItemLabel.FORMULA:
                    num_formulas += 1
                elif element.label == DocItemLabel.CODE:
                    num_codes += 1

        num_pages = len(conv_res.document.pages)
        num_tables = len(conv_res.document.tables)
        num_elements = len(conv_res.document.texts)
        num_pictures = len(conv_res.document.pictures)

        # All features
        features = [
            num_pages,
            num_elements,
            num_tables,
            num_pictures,
            num_formulas,
            num_codes,
            *picture_classes.values(),
        ]

        doc_hash = (
            conv_res.document.origin.binary_hash
            if conv_res.document.origin
            else "unknown_hash"
        )
        doc_json = json.dumps(conv_res.document.export_to_dict())

        pdf_byte_array: bytearray | None = None
        if os.path.exists(conv_res.input.file):
            with open(conv_res.input.file, "rb") as file:
                pdf_byte_array = bytearray(file.read())

        result_table.append(
            {
                "filename": filename,
                "pdf": pdf_byte_array,
                "doc_hash": doc_hash,
                "document": doc_json,
                "page_images": page_images,
                "features": features,
                "doctags": str.encode(conv_res.document.export_to_document_tokens()),
            }
        )

        pd_df = pd.json_normalize(result_table)
        pd_df = pd_dataframe._append(pd_df)

        return pd_df

    def upload_parquet_file(self, pd_dataframe: DataFrame):
        # Variables to track the file writing process
        file_index = 0
        current_file_size = 0
        current_df = pd.DataFrame()
        # Manifest dictionary
        manifest = {}
        # Current time
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        while len(pd_dataframe) > 0:
            # Get a chunk of the DataFrame that fits within the file size limit
            chunk_size = min(
                len(pd_dataframe), MAX_PARQUET_FILE_SIZE // (current_file_size + 1)
            )

            # If the chunk size is 0, it means the current file size has exceeded the limit
            if chunk_size == 0:
                with tempfile.NamedTemporaryFile(
                    suffix=f".parquet_{file_index}", dir=self.scratch_dir
                ) as temp_file:
                    pd_dataframe.to_parquet(temp_file)
                    current_file_size = temp_file.seek(0, 2)
                    file_index += 1

                    parquet_file_name = f"{timestamp}_{file_index}.parquet"
                    target_key = f"parquet/{parquet_file_name}"
                    self._target_processor.upload_file(
                        filename=temp_file.name,
                        target_filename=target_key,
                        content_type="application/vnd.apache.parquet",
                    )

                    manifest[f"{parquet_file_name}"] = {
                        "filename": pd_dataframe["filename"].tolist(),
                        "doc_hash": pd_dataframe["doc_hash"].tolist(),
                        "row_number": 3,
                        "timestamp": timestamp,
                    }

                pd_dataframe = pd.DataFrame()
            else:
                # Get the current chunk of the DataFrame
                current_df = pd_dataframe.iloc[:chunk_size]
                pd_dataframe = pd_dataframe.iloc[chunk_size:]

                with tempfile.NamedTemporaryFile(
                    suffix=f".parquet_{file_index}", dir=self.scratch_dir
                ) as temp_file:
                    current_df.to_parquet(temp_file.name)
                    current_file_size = temp_file.seek(0, 2)
                    file_index += 1

                    parquet_file_name = f"{timestamp}_{file_index}.parquet"
                    target_key = f"parquet/{parquet_file_name}"
                    self._target_processor.upload_file(
                        filename=temp_file.name,
                        target_filename=target_key,
                        content_type="application/vnd.apache.parquet",
                    )

                    manifest[f"{parquet_file_name}"] = {
                        "filenames": current_df["filename"].tolist(),
                        "doc_hashes": current_df["doc_hash"].tolist(),
                        "row_number": 3,
                        "timestamp": timestamp,
                    }

        logging.info(f"Total parquet files uploaded: {file_index}")

        # Export manifest file:
        with tempfile.NamedTemporaryFile(
            suffix=".json", dir=self.scratch_dir
        ) as temp_file_json:
            with open(temp_file_json.name, "w") as file:
                json.dump(manifest, file, indent=4)
            self._target_processor.upload_file(
                filename=temp_file_json.name,
                target_filename=f"manifest/{timestamp}.json",
                content_type="application/json",
            )
