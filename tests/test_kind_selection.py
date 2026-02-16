"""Tests for kind selection feature in table structure and layout."""

import pytest

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions


class TestKindSelection:
    """Test kind selection for table structure and layout."""

    def test_default_table_structure_kind(self):
        """Test that default table structure kind is used when no custom config."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions()

        # Should use default kind without error
        options = manager._parse_table_structure_options(request)
        assert options is not None

    def test_custom_table_structure_kind(self):
        """Test custom table structure kind with config."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            table_structure_custom_config={
                "kind": "docling_tableformer",
                "mode": "fast",
                "do_cell_matching": False,
            }
        )

        # Should create options with custom config
        options = manager._parse_table_structure_options(request)
        assert options is not None

    def test_table_structure_kind_missing_kind_field(self):
        """Test that missing 'kind' field raises error."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            table_structure_custom_config={
                "mode": "fast",
                # Missing 'kind' field
            }
        )

        with pytest.raises(ValueError, match="must include a 'kind' field"):
            manager._parse_table_structure_options(request)

    def test_table_structure_kind_not_allowed(self):
        """Test that non-allowed kind raises error."""
        config = DoclingConverterManagerConfig(
            allowed_table_structure_kinds=["docling_tableformer"]
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            table_structure_custom_config={
                "kind": "unapproved_plugin",
                "some_param": "value",
            }
        )

        with pytest.raises(ValueError, match="not allowed by administrator"):
            manager._parse_table_structure_options(request)

    def test_default_layout_kind(self):
        """Test that default layout kind is used when no custom config."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions()

        # Should use default kind without error
        options = manager._parse_layout_options(request)
        assert options is not None

    def test_custom_layout_kind(self):
        """Test custom layout kind with config."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            layout_custom_config={
                "kind": "docling_layout_default",
                "model_name": "egret_large",
            }
        )

        # Should create options with custom config
        options = manager._parse_layout_options(request)
        assert options is not None

    def test_layout_kind_missing_kind_field(self):
        """Test that missing 'kind' field raises error."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            layout_custom_config={
                "model_name": "egret_large",
                # Missing 'kind' field
            }
        )

        with pytest.raises(ValueError, match="must include a 'kind' field"):
            manager._parse_layout_options(request)

    def test_layout_kind_not_allowed(self):
        """Test that non-allowed kind raises error."""
        config = DoclingConverterManagerConfig(
            allowed_layout_kinds=["docling_layout_default"]
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            layout_custom_config={
                "kind": "unapproved_plugin",
                "some_param": "value",
            }
        )

        with pytest.raises(ValueError, match="not allowed by administrator"):
            manager._parse_layout_options(request)

    def test_default_kind_always_allowed(self):
        """Test that default kind is always allowed even with allowlist."""
        config = DoclingConverterManagerConfig(
            default_table_structure_kind="docling_tableformer",
            allowed_table_structure_kinds=["other_kind"],  # Default not in list
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            table_structure_custom_config={
                "kind": "docling_tableformer",  # This is the default
                "mode": "accurate",
            }
        )

        # Should not raise error - default is always allowed
        options = manager._parse_table_structure_options(request)
        assert options is not None

    def test_kind_registries_built(self):
        """Test that kind registries are built on initialization."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        # Check that registries exist
        assert hasattr(manager, "available_table_structure_kinds")
        assert hasattr(manager, "available_layout_kinds")

        # Check that they are lists
        assert isinstance(manager.available_table_structure_kinds, list)
        assert isinstance(manager.available_layout_kinds, list)

    def test_validate_kind_allowed_with_none_allowlist(self):
        """Test that None allowlist allows all kinds."""
        config = DoclingConverterManagerConfig(
            allowed_table_structure_kinds=None  # Allow all
        )
        manager = DoclingConverterManager(config)

        # Should not raise error for any kind
        manager._validate_kind_allowed(
            "any_kind",
            config.allowed_table_structure_kinds,
            config.default_table_structure_kind,
            "table structure",
        )

    def test_backwards_compatibility_legacy_fields(self):
        """Test that legacy table_mode and table_cell_matching still work."""
        # Use legacy fields without custom config
        request = ConvertDocumentsOptions(
            table_mode="fast",
            table_cell_matching=False,
        )

        # Should work with legacy approach in _parse_standard_pdf_opts
        # This is tested indirectly through the pipeline options
        assert request.table_mode == "fast"
        assert request.table_cell_matching is False
