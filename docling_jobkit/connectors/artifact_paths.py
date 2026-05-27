from hashlib import sha256
from typing import Literal

from docling.datamodel.service.sources import S3Coordinates

# TODO: add "doclang" once batch artifact typing supports it end-to-end.
ArtifactType = Literal["json", "html", "markdown", "text", "doctags", "resource_bundle"]


def hash_path_component(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def build_s3_source_key(source: S3Coordinates) -> str:
    public_identity = "|".join(
        [
            source.endpoint.strip(),
            source.bucket.strip(),
            source.key_prefix.strip("/"),
        ]
    )
    return hash_path_component(public_identity)
