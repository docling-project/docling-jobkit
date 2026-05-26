from hashlib import sha256
from pathlib import Path
from typing import Literal

ArtifactType = Literal["json", "html", "markdown", "text", "doctags", "resource_bundle"]


def build_source_key(source_index: int, source_uri: str) -> str:
    return f"{source_index:06d}-{sha256(source_uri.encode()).hexdigest()[:12]}"


def build_task_scoped_artifact_path(
    task_id: str, source_index: int, source_uri: str, artifact_filename: str
) -> str:
    return f"{task_id}/{build_source_key(source_index, source_uri)}/{artifact_filename}"


def infer_artifact_type(artifact_filename: str) -> ArtifactType:
    suffix = Path(artifact_filename).suffix.lower()
    if artifact_filename.endswith("_bundle.zip"):
        return "resource_bundle"
    if suffix == ".json":
        return "json"
    if suffix == ".html":
        return "html"
    if suffix == ".md":
        return "markdown"
    if suffix == ".txt":
        return "text"
    if suffix == ".doctags":
        return "doctags"
    raise ValueError(f"Unsupported artifact filename: {artifact_filename}")
