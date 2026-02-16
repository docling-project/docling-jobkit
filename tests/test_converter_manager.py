"""Unit tests for DoclingConverterManager preset and engine control."""

import pytest

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions


class TestPresetRegistryBuilding:
    """Test that preset registries are built correctly."""

    def test_default_preset_always_included(self):
        """Test that 'default' preset is always in the registry."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
        )
        manager = DoclingConverterManager(config)

        assert "default" in manager.vlm_preset_registry
        assert manager.vlm_preset_registry["default"]["source"] == "docling"
        assert manager.vlm_preset_registry["default"]["preset_id"] == "granite_docling"

    def test_allowed_presets_restriction(self):
        """Test that only allowed presets are in the registry."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
            allowed_vlm_presets=["smoldocling"],
        )
        manager = DoclingConverterManager(config)

        # "default" should always be there
        assert "default" in manager.vlm_preset_registry
        # "smoldocling" should be allowed
        assert "smoldocling" in manager.vlm_preset_registry
        # Other presets should not be in registry
        # (We can't test for specific presets without knowing all Docling presets)

    def test_custom_presets_added(self):
        """Test that custom presets are added to the registry."""
        custom_preset = {
            "engine_type": "api_generic",
            "url": "http://test.com",
        }
        config = DoclingConverterManagerConfig(
            custom_vlm_presets={"my_custom": custom_preset},
        )
        manager = DoclingConverterManager(config)

        assert "my_custom" in manager.vlm_preset_registry
        assert manager.vlm_preset_registry["my_custom"]["source"] == "custom"
        assert manager.vlm_preset_registry["my_custom"]["options"] == custom_preset

    def test_picture_description_registry(self):
        """Test picture description preset registry."""
        config = DoclingConverterManagerConfig(
            default_picture_description_preset="smolvlm",
        )
        manager = DoclingConverterManager(config)

        assert "default" in manager.picture_description_preset_registry
        assert (
            manager.picture_description_preset_registry["default"]["preset_id"]
            == "smolvlm"
        )

    def test_code_formula_registry(self):
        """Test code/formula preset registry."""
        config = DoclingConverterManagerConfig(
            default_code_formula_preset="default",
        )
        manager = DoclingConverterManager(config)

        assert "default" in manager.code_formula_preset_registry


class TestPresetValidation:
    """Test that invalid presets are rejected."""

    def test_invalid_vlm_preset_rejected(self):
        """Test that invalid VLM preset raises error."""
        config = DoclingConverterManagerConfig(
            allowed_vlm_presets=["granite_docling"],
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_preset(
                "nonexistent_preset", manager.vlm_preset_registry, "VLM"
            )

    def test_valid_vlm_preset_accepted(self):
        """Test that valid VLM preset is accepted."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
            allowed_vlm_presets=["granite_vision"],
        )
        manager = DoclingConverterManager(config)

        # Should not raise - "default" always works, and "granite_vision" is in allowed list
        manager._validate_preset("default", manager.vlm_preset_registry, "VLM")
        manager._validate_preset("granite_vision", manager.vlm_preset_registry, "VLM")

    def test_invalid_picture_description_preset_rejected(self):
        """Test that invalid picture description preset raises error."""
        config = DoclingConverterManagerConfig(
            allowed_picture_description_presets=["smolvlm"],
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_preset(
                "nonexistent",
                manager.picture_description_preset_registry,
                "Picture description",
            )

    def test_invalid_code_formula_preset_rejected(self):
        """Test that invalid code/formula preset raises error."""
        config = DoclingConverterManagerConfig(
            allowed_code_formula_presets=["default"],
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_preset(
                "nonexistent", manager.code_formula_preset_registry, "Code/formula"
            )


class TestCustomConfigValidation:
    """Test that custom configs are validated."""

    def test_custom_vlm_config_not_allowed(self):
        """Test that custom VLM config is rejected when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_vlm_config=False,
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_custom_config_allowed("vlm")

    def test_custom_vlm_config_allowed(self):
        """Test that custom VLM config is accepted when allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_vlm_config=True,
        )
        manager = DoclingConverterManager(config)

        # Should not raise
        manager._validate_custom_config_allowed("vlm")

    def test_custom_picture_description_config_not_allowed(self):
        """Test that custom picture description config is rejected when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_picture_description_config=False,
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_custom_config_allowed("picture_description")

    def test_custom_code_formula_config_not_allowed(self):
        """Test that custom code/formula config is rejected when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_code_formula_config=False,
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_custom_config_allowed("code_formula")


class TestEngineRestriction:
    """Test that engine restrictions are enforced."""

    def test_engine_not_in_allowed_list(self):
        """Test that disallowed engine raises error."""
        config = DoclingConverterManagerConfig(
            allowed_vlm_engines=["transformers"],
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_engine_allowed("mlx", config.allowed_vlm_engines)

    def test_engine_in_allowed_list(self):
        """Test that allowed engine is accepted."""
        config = DoclingConverterManagerConfig(
            allowed_vlm_engines=["transformers", "mlx"],
        )
        manager = DoclingConverterManager(config)

        # Should not raise
        manager._validate_engine_allowed("transformers", config.allowed_vlm_engines)
        manager._validate_engine_allowed("mlx", config.allowed_vlm_engines)

    def test_no_engine_restriction(self):
        """Test that all engines are allowed when restriction is None."""
        config = DoclingConverterManagerConfig(
            allowed_vlm_engines=None,
        )
        manager = DoclingConverterManager(config)

        # Should not raise for any engine
        manager._validate_engine_allowed("transformers", config.allowed_vlm_engines)
        manager._validate_engine_allowed("mlx", config.allowed_vlm_engines)
        manager._validate_engine_allowed("api_generic", config.allowed_vlm_engines)


class TestOptionsParsingPreset:
    """Test options parsing from presets."""

    def test_parse_vlm_options_with_preset(self):
        """Test parsing VLM options from preset."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            vlm_pipeline_preset="default",
        )

        options = manager._parse_vlm_options(request)
        assert options is not None

    def test_parse_vlm_options_without_preset(self):
        """Test parsing VLM options without preset returns None."""
        config = DoclingConverterManagerConfig()
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions()

        options = manager._parse_vlm_options(request)
        assert options is None

    def test_parse_picture_description_options_with_preset(self):
        """Test parsing picture description options from preset."""
        config = DoclingConverterManagerConfig(
            default_picture_description_preset="smolvlm",
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            picture_description_preset="default",
        )

        options = manager._parse_picture_description_options(request)
        assert options is not None


class TestOptionsParsingCustomConfig:
    """Test options parsing from custom configs."""

    def test_parse_vlm_options_with_custom_config_dict(self):
        """Test parsing VLM options from custom config (dict)."""
        config = DoclingConverterManagerConfig(
            allow_custom_vlm_config=True,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            vlm_pipeline_custom_config={
                "model_spec": {
                    "name": "Custom Test Model",
                    "default_repo_id": "test-model",
                    "prompt": "Convert this page to docling.",
                    "response_format": "doctags",
                },
                "engine_options": {
                    "engine_type": "transformers",
                    "device": None,
                    "load_in_8bit": True,
                },
                "scale": 2.0,
                "batch_size": 1,
            }
        )

        options = manager._parse_vlm_options(request)
        assert options is not None

    def test_parse_vlm_options_custom_config_not_allowed(self):
        """Test that custom config raises error when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_vlm_config=False,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            vlm_pipeline_custom_config={
                "engine_type": "transformers",
                "repo_id": "test-model",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_vlm_options(request)

    def test_parse_vlm_options_engine_not_allowed(self):
        """Test that disallowed engine in custom config raises error."""
        config = DoclingConverterManagerConfig(
            allow_custom_vlm_config=True,
            allowed_vlm_engines=["api_generic"],
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            vlm_pipeline_custom_config={
                "engine_type": "transformers",
                "repo_id": "test-model",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_vlm_options(request)


class TestGetVlmOptionsFromPreset:
    """Test getting VLM options from preset."""

    def test_get_docling_preset(self):
        """Test getting options from Docling built-in preset."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
        )
        manager = DoclingConverterManager(config)

        from docling.datamodel.pipeline_options import VlmConvertOptions

        options = manager._get_options_from_preset(
            "default",
            manager.vlm_preset_registry,
            "VLM",
            manager.config.allowed_vlm_engines,
            VlmConvertOptions.from_preset,
        )
        assert options is not None
        # Should be a VlmConvertOptions instance
        assert hasattr(options, "engine_options")

    def test_get_custom_preset(self):
        """Test getting options from custom preset."""
        from docling.datamodel.pipeline_options import VlmConvertOptions

        custom_options = VlmConvertOptions.from_preset("granite_docling")
        config = DoclingConverterManagerConfig(
            custom_vlm_presets={"my_preset": custom_options},
        )
        manager = DoclingConverterManager(config)

        options = manager._get_options_from_preset(
            "my_preset",
            manager.vlm_preset_registry,
            "VLM",
            manager.config.allowed_vlm_engines,
            VlmConvertOptions.from_preset,
        )
        assert options is not None
        assert options == custom_options
