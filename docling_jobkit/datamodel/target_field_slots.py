from typing import Any, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import OutputFormat

# Maximum value of a 64-bit signed long (OpenSearch / Elasticsearch limit).
_INT64_MAX = (1 << 63) - 1
_INT64_MIN = -(1 << 63)


def coerce_large_ints(obj: Any) -> Any:
    """Recursively walk a JSON-like structure and stringify any int that
    falls outside the 64-bit signed long range accepted by OpenSearch /
    Elasticsearch.  Other types are returned unchanged."""
    if isinstance(obj, dict):
        return {k: coerce_large_ints(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [coerce_large_ints(v) for v in obj]
    if isinstance(obj, int) and not isinstance(obj, bool):
        if obj > _INT64_MAX or obj < _INT64_MIN:
            return str(obj)
    return obj


# Canonical mapping from OutputFormat to MIME content-type.
# DOCTAGS shares text/plain with TEXT — there is no distinct registered MIME
# type for doctags.  Consumers that need to distinguish them must do so by
# context (e.g. file extension) rather than MIME type alone.
OUTPUT_FORMAT_MIME: dict[OutputFormat, str] = {
    OutputFormat.JSON: "application/json",
    OutputFormat.HTML: "text/html",
    OutputFormat.TEXT: "text/plain",
    OutputFormat.DOCTAGS: "text/plain",
    OutputFormat.MARKDOWN: "text/markdown",
    OutputFormat.DOCLANG: "application/xml",
    OutputFormat.DCLX: "application/zip",
}


class FieldMappings(BaseModel):
    """Maps output format names to DB field/column names."""

    mappings: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Keys are OutputFormat values ('MARKDOWN', 'JSON', 'DOCTAGS', etc.). "
            "Values are the target field/column name. "
            "Unspecified formats are not written."
        ),
        examples=[
            {"MARKDOWN": "text", "JSON": "doc_json"},
        ],
    )

    coerce_large_ints_to_str: bool = Field(
        default=False,
        description=(
            "When True, any integer in the serialised JSON document that exceeds "
            "the 64-bit signed long range (e.g. DoclingDocument.origin.binary_hash) "
            "is converted to a string before indexing.  Enable for targets whose "
            "schema maps numeric fields as 'long' (OpenSearch, Elasticsearch)."
        ),
    )


class ChunkFieldSlots(BaseModel):
    """Field mapping for chunk-level records."""

    text_field: str = "text"
    metadata_field: str = "metadata"
    page_field: Optional[str] = None
    headings_field: Optional[str] = None
    doc_id_field: str = "doc_id"
    chunk_index_field: str = "chunk_index"
    coerce_large_ints_to_str: bool = Field(
        default=False,
        description=(
            "When True, any integer in the serialised chunk metadata that exceeds "
            "the 64-bit signed long range is converted to a string before indexing. "
            "Enable for targets whose schema maps numeric fields as 'long' "
            "(OpenSearch, Elasticsearch)."
        ),
    )


__all__ = [
    "OUTPUT_FORMAT_MIME",
    "ChunkFieldSlots",
    "FieldMappings",
]
