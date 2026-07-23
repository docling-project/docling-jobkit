"""Shared document-export primitives used by every execution path.

Both the CLI and the orchestrators must turn converted documents into artifact
files and push them through a :class:`BaseTargetProcessor`. Centralizing that
materialize→upload step here means the *same code* runs whether a connector is
exercised locally (CLI) or on distributed compute (Ray/RQ/local orchestrator),
and keeps memory bounded: callers stream documents one at a time and release each
document's heavy ``DoclingDocument`` reference right after its artifacts are sent.

Only :func:`export_documents_to_target` is public; the other helpers are internal
building blocks (underscore-prefixed) that ``convert.results`` reuses directly.
"""

import logging
import shutil
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from docling.datamodel.base_models import OutputFormat
from docling.datamodel.document import ConversionStatus
from docling_core.types.doc import ImageRefMode

from docling_jobkit.connectors.artifact_paths import ArtifactType
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.target_field_slots import OUTPUT_FORMAT_MIME

_log = logging.getLogger(__name__)


@dataclass
class _ExportedArtifactFile:
    artifact_type: ArtifactType
    path: Path
    target_filename: str
    mime_type: str


def _is_exportable_status(status: ConversionStatus) -> bool:
    return status in (ConversionStatus.SUCCESS, ConversionStatus.PARTIAL_SUCCESS)


def _materialize_document_exports(
    exportable_document: ExportableDocument,
    output_dir: Path,
    *,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    bundle_resources: bool,
) -> list[_ExportedArtifactFile]:
    """Write the requested export formats to ``output_dir`` and return their files.

    When ``bundle_resources`` is set and referenced artifacts (images) were
    written, a self-contained ``<doc>_bundle.zip`` is also produced.
    """
    if not (
        _is_exportable_status(exportable_document.status)
        and exportable_document.document is not None
    ):
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = Path("artifacts")
    generated: list[_ExportedArtifactFile] = []
    doc_filename = exportable_document.file.stem

    if export_json:
        fname = output_dir / f"{doc_filename}.json"
        _log.info(f"writing JSON output to {fname}")
        exportable_document.document.save_as_json(
            filename=fname,
            image_mode=image_export_mode,
            artifacts_dir=artifacts_dir,
        )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="json",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.JSON],
            )
        )

    if export_html:
        fname = output_dir / f"{doc_filename}.html"
        _log.info(f"writing HTML output to {fname}")
        exportable_document.document.save_as_html(
            filename=fname,
            image_mode=image_export_mode,
            artifacts_dir=artifacts_dir,
        )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="html",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.HTML],
            )
        )

    if export_txt:
        fname = output_dir / f"{doc_filename}.txt"
        _log.info(f"writing TXT output to {fname}")
        exportable_document.document.save_as_markdown(
            filename=fname,
            strict_text=True,
            image_mode=ImageRefMode.PLACEHOLDER,
        )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="text",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.TEXT],
            )
        )

    if export_md:
        fname = output_dir / f"{doc_filename}.md"
        _log.info(f"writing Markdown output to {fname}")
        exportable_document.document.save_as_markdown(
            filename=fname,
            artifacts_dir=artifacts_dir,
            image_mode=image_export_mode,
            page_break_placeholder=md_page_break_placeholder or None,
        )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="markdown",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.MARKDOWN],
            )
        )

    if export_doctags:
        fname = output_dir / f"{doc_filename}.doctags"
        _log.info(f"writing Doc Tags output to {fname}")
        exportable_document.document.save_as_doctags(filename=fname)
        generated.append(
            _ExportedArtifactFile(
                artifact_type="doctags",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.DOCTAGS],
            )
        )

    if export_doclang:
        fname = output_dir / f"{doc_filename}.dclg"
        _log.info(f"writing DocLang output to {fname}")
        fname.write_text(
            exportable_document.document.export_to_doclang() + "\n", encoding="utf-8"
        )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="doclang",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.DOCLANG],
            )
        )

    if export_dclx:
        fname = output_dir / f"{doc_filename}.dclx"
        _log.info(f"writing DCLX archive output to {fname}")
        exportable_document.document.save_as_doclang_archive(filename=fname)
        generated.append(
            _ExportedArtifactFile(
                artifact_type="dclx",
                path=fname,
                target_filename=fname.name,
                mime_type=OUTPUT_FORMAT_MIME[OutputFormat.DCLX],
            )
        )

    artifacts_path = output_dir / artifacts_dir
    if bundle_resources and artifacts_path.exists() and any(artifacts_path.iterdir()):
        bundle_path = output_dir / f"{doc_filename}_bundle.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as bundle_zip:
            # Place the exported documents alongside the artifacts directory,
            # using the same relative layout (`<artifacts_dir>/<file>`) that
            # the documents themselves reference, so the bundle is
            # self-contained and resolvable without any path rewriting.
            for artifact in generated:
                bundle_zip.write(artifact.path, arcname=artifact.target_filename)
            for artifact_file in sorted(artifacts_path.rglob("*")):
                if artifact_file.is_file():
                    bundle_zip.write(
                        artifact_file,
                        arcname=str(
                            artifacts_dir / artifact_file.relative_to(artifacts_path)
                        ),
                    )
        generated.append(
            _ExportedArtifactFile(
                artifact_type="resource_bundle",
                path=bundle_path,
                target_filename=bundle_path.name,
                mime_type="application/zip",
            )
        )

    return generated


def _upload_exportable_document(
    *,
    target_processor: BaseTargetProcessor,
    exportable_document: ExportableDocument,
    document_dir: Path,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    target_filename_fn: Callable[[str], str],
    bundle_resources: bool = True,
) -> list[_ExportedArtifactFile]:
    """Materialize one document's exports and upload them via ``target_processor``.

    ``target_filename_fn(artifact_filename) -> target_key`` is the single seam
    that controls the final layout, letting each caller keep its own convention
    (e.g. ``<source_key>/<artifact>`` for orchestrators or ``json/<name>.json``
    for the CLI) while the materialize+upload code itself stays shared.
    """
    artifacts = _materialize_document_exports(
        exportable_document,
        document_dir,
        export_json=export_json,
        export_html=export_html,
        export_md=export_md,
        export_txt=export_txt,
        export_doctags=export_doctags,
        export_doclang=export_doclang,
        export_dclx=export_dclx,
        image_export_mode=image_export_mode,
        md_page_break_placeholder=md_page_break_placeholder,
        bundle_resources=bundle_resources,
    )
    for artifact in artifacts:
        target_processor.upload_file(
            filename=artifact.path,
            target_filename=target_filename_fn(artifact.target_filename),
            content_type=artifact.mime_type,
        )
    return artifacts


def _release_exportable_document_references(
    *exportable_documents: ExportableDocument,
) -> None:
    for exportable_document in exportable_documents:
        exportable_document.document = None


def _cleanup_document_output_dir(document_dir: Path) -> None:
    shutil.rmtree(document_dir, ignore_errors=True)


def export_documents_to_target(
    *,
    exportable_documents: Iterable[ExportableDocument],
    target_processor: BaseTargetProcessor,
    output_dir: Path,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    target_filename_fn: Callable[[ExportableDocument, str], str],
    bundle_resources: bool = True,
    on_document_uploaded: Callable[[ExportableDocument], None] | None = None,
) -> int:
    """Stream documents to ``target_processor``, releasing each after upload.

    Consumes ``exportable_documents`` lazily so peak memory stays bounded to a
    single document: each one is materialized into its own subdirectory of
    ``output_dir``, uploaded, then its ``DoclingDocument`` reference and scratch
    files are released before the next is pulled. Returns the number of
    exportable documents uploaded.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    uploaded = 0
    for index, exportable_document in enumerate(exportable_documents):
        document_index = (
            exportable_document.source_index
            if exportable_document.source_index is not None
            else index
        )
        document_dir = output_dir / f"{document_index:06d}"

        def _document_target_filename(
            artifact_filename: str, _doc: ExportableDocument = exportable_document
        ) -> str:
            return target_filename_fn(_doc, artifact_filename)

        try:
            artifacts = _upload_exportable_document(
                target_processor=target_processor,
                exportable_document=exportable_document,
                document_dir=document_dir,
                export_json=export_json,
                export_html=export_html,
                export_md=export_md,
                export_txt=export_txt,
                export_doctags=export_doctags,
                export_doclang=export_doclang,
                export_dclx=export_dclx,
                image_export_mode=image_export_mode,
                md_page_break_placeholder=md_page_break_placeholder,
                target_filename_fn=_document_target_filename,
                bundle_resources=bundle_resources,
            )
            if artifacts:
                uploaded += 1
            if on_document_uploaded is not None:
                on_document_uploaded(exportable_document)
        finally:
            _release_exportable_document_references(exportable_document)
            _cleanup_document_output_dir(document_dir)
    return uploaded
