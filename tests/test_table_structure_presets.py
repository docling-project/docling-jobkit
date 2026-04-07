"""Tests for table structure preset functionality."""

import pytest

from docling.datamodel.pipeline_options import TableFormerMode

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions


def test_default_preset_used():
    """Test that default preset is used when no options specified."""
    config = DoclingConverterManagerConfig(
        default_table_structure_preset="tableformer_v1_accurate"
    )
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions()
    options = manager._parse_table_structure_options(request)
    assert options is not None
    assert options.mode.value == "accurate"


def test_explicit_preset():
    """Test explicit preset selection using built-in preset."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(table_structure_preset="tableformer_v1_fast")
    options = manager._parse_table_structure_options(request)
    assert options.mode.value == "fast"


def test_custom_preset_from_config():
    """Test custom preset defined in manager config."""
    config = DoclingConverterManagerConfig(
        custom_table_structure_presets={
            "custom_accurate": {
                "kind": "docling_tableformer",
                "mode": "accurate",
                "do_cell_matching": True,
            }
        }
    )
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(table_structure_preset="custom_accurate")
    options = manager._parse_table_structure_options(request)
    assert options is not None
    assert options.mode.value == "accurate"
    assert options.do_cell_matching is True


def test_legacy_fields_still_work():
    """Test backward compatibility with legacy fields."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(
        table_mode=TableFormerMode.FAST, table_cell_matching=False
    )
    options = manager._parse_table_structure_options(request)
    assert options.mode.value == "fast"
    assert options.do_cell_matching is False


def test_custom_config_overrides_preset():
    """Test that custom config takes precedence over preset."""
    config = DoclingConverterManagerConfig(allow_custom_table_structure_config=True)
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(
        table_structure_preset="tableformer_v1_fast",
        table_structure_custom_config={
            "kind": "docling_tableformer",
            "mode": "accurate",
        },
    )
    options = manager._parse_table_structure_options(request)
    assert options.mode.value == "accurate"


def test_preset_filtering():
    """Test that only allowed presets can be used."""
    config = DoclingConverterManagerConfig(
        allowed_table_structure_presets=["tableformer_v1_accurate"],
    )
    manager = DoclingConverterManager(config)

    # tableformer_v1_fast should not be in registry because it's not in allowed list
    request = ConvertDocumentsOptions(table_structure_preset="tableformer_v1_fast")
    with pytest.raises(ValueError, match="Unknown table structure preset"):
        manager._parse_table_structure_options(request)

    # But tableformer_v1_accurate should work
    request2 = ConvertDocumentsOptions(table_structure_preset="tableformer_v1_accurate")
    options = manager._parse_table_structure_options(request2)
    assert options is not None
    assert options.mode.value == "accurate"


def test_unknown_preset_raises_error():
    """Test that unknown preset raises error."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(table_structure_preset="nonexistent_preset")
    with pytest.raises(ValueError, match="Unknown table structure preset"):
        manager._parse_table_structure_options(request)


def test_built_in_presets_available():
    """Test that built-in presets are available."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)

    # Check that built-in presets are in the registry
    expected_presets = [
        "tableformer_v1_accurate",
        "tableformer_v1_fast",
        "tableformer_v2",
    ]
    for preset_id in expected_presets:
        assert preset_id in manager.table_structure_preset_registry
        preset_info = manager.table_structure_preset_registry[preset_id]
        assert preset_info["source"] == "custom"
        assert "options" in preset_info


def test_default_preset_resolves_correctly():
    """Test that 'default' preset resolves to the configured default."""
    config = DoclingConverterManagerConfig(
        default_table_structure_preset="tableformer_v1_fast"
    )
    manager = DoclingConverterManager(config)

    # Using "default" should resolve to tableformer_v1_fast
    request = ConvertDocumentsOptions(table_structure_preset="default")
    options = manager._parse_table_structure_options(request)
    assert options.mode.value == "fast"


def test_legacy_fields_with_non_default_values_skip_preset():
    """Test that non-default legacy fields skip preset usage."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(
        table_mode=TableFormerMode.FAST,  # Non-default value
        table_cell_matching=False,  # Non-default value
    )
    options = manager._parse_table_structure_options(request)
    # Should use legacy fields, not preset
    assert options.mode.value == "fast"
    assert options.do_cell_matching is False


def test_preset_with_legacy_fields():
    """Test that custom preset ignores legacy fields."""
    config = DoclingConverterManagerConfig(
        custom_table_structure_presets={
            "test_preset": {
                "kind": "docling_tableformer",
                "mode": "accurate",
                "do_cell_matching": True,
            }
        }
    )
    manager = DoclingConverterManager(config)
    request = ConvertDocumentsOptions(
        table_structure_preset="test_preset",
        table_mode=TableFormerMode.FAST,  # Should be ignored for custom presets
        table_cell_matching=False,  # Should be ignored for custom presets
    )
    options = manager._parse_table_structure_options(request)
    # Custom preset values should be used, not legacy fields
    assert options.mode.value == "accurate"


def test_default_legacy_fields_dont_pass_parameters():
    """Test that default legacy fields don't pass unnecessary parameters to factory."""
    config = DoclingConverterManagerConfig()
    manager = DoclingConverterManager(config)

    # When legacy fields are at defaults, they shouldn't be passed to factory
    # This allows kinds that don't support these parameters to work
    request = ConvertDocumentsOptions()  # All defaults
    options = manager._parse_table_structure_options(request)

    # Should work without errors even if the kind doesn't support mode/do_cell_matching
    assert options is not None
    assert options.do_cell_matching is True
