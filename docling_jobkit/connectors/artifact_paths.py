from hashlib import sha256
from pathlib import Path
from typing import Literal

from docling.datamodel.service.sources import S3Coordinates

ArtifactType = Literal["json", "html", "markdown", "text", "doctags", "resource_bundle"]


def _hash_component(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def build_source_key(source_uri: str) -> str:
    return _hash_component(source_uri)


def build_s3_source_key(source: S3Coordinates) -> str:
    public_identity = "|".join(
        [
            source.endpoint.strip(),
            source.bucket.strip(),
            source.key_prefix.strip("/"),
        ]
    )
    return _hash_component(public_identity)


def build_task_scoped_artifact_path(source_key: str, artifact_filename: str) -> str:
    # S3Target writes into a user-owned bucket, so this helper only adds the
    # per-source hash segment. The user-provided target key_prefix is prepended
    # later by S3TargetProcessor._build_full_key.
    return f"{source_key}/{artifact_filename}"


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
