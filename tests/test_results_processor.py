from pathlib import Path

from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult, InputDocument, _DummyBackend
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.convert.results_processor import ResultsProcessor


class _RecordingTargetProcessor(BaseTargetProcessor):
    def __init__(self):
        super().__init__()
        self.uploads: list[tuple[str, str, str | bytes]] = []

    def _initialize(self) -> None:
        return None

    def _finalize(self) -> None:
        return None

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        self.uploads.append(
            (target_filename, content_type, Path(filename).read_text(encoding="utf-8"))
        )

    def upload_object(
        self,
        obj: str | bytes,
        target_filename: str,
        content_type: str,
    ) -> None:
        self.uploads.append((target_filename, content_type, obj))


class _FakeDoc(DoclingDocument):
    def save_as_json(self, filename, image_mode, artifacts_dir=None):
        del image_mode, artifacts_dir
        Path(filename).write_text('{"ok": true}', encoding="utf-8")

    def export_to_doctags(self):
        return "<doc/>"

    def export_to_markdown(self, **_kwargs):
        return "# title"

    def save_as_html(self, filename):
        Path(filename).write_text("<p>ok</p>", encoding="utf-8")

    def export_to_text(self):
        return "plain text"


def test_results_processor_uses_narrow_base_target_processor_contract(tmp_path: Path):
    input_path = tmp_path / "input.pdf"
    input_path.write_bytes(b"%PDF-1.4")

    input_doc = InputDocument(
        path_or_stream=input_path,
        format=InputFormat.PDF,
        backend=_DummyBackend,
    )
    conv_res = ConversionResult(
        input=input_doc,
        status=ConversionStatus.SUCCESS,
        document=_FakeDoc.model_construct(),
    )

    target_processor = _RecordingTargetProcessor()
    with target_processor:
        results = list(
            ResultsProcessor(
                target_processors=[target_processor],
                to_formats=["json", "doctags", "md", "html", "text"],
                scratch_dir=tmp_path / "scratch",
            ).process_documents([conv_res])
        )

    assert results == [f"{input_doc.document_hash} - SUCCESS"]
    uploaded_targets = sorted(
        target for target, _content_type, _obj in target_processor.uploads
    )
    assert len(uploaded_targets) == 6
    assert uploaded_targets[0].endswith("input.doctags.txt")
    assert uploaded_targets[1].endswith("input.html")
    assert uploaded_targets[2].endswith("input.json")
    assert uploaded_targets[3].endswith("input.md")
    assert uploaded_targets[4].endswith("input.pdf")
    assert uploaded_targets[5].endswith("input.txt")
