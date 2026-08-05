from hashlib import sha256
from typing import Literal

ArtifactType = Literal[
    "json",
    "html",
    "markdown",
    "text",
    "doctags",
    "doclang",
    "dclx",
    "resource_bundle",
    "chunks",
]


def hash_path_component(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]
