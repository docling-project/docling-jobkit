"""Compatibility re-exports for source wire types now owned by docling."""

from docling.datamodel.service.sources import FileSource, HttpSource

__all__ = ["FileSource", "HttpSource"]
