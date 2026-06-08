from dataclasses import dataclass


@dataclass(frozen=True)
class SourceIdentity:
    source_index: int
    source_uri: str
    source_key: str
