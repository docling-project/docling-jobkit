"""Unit tests for ConvertDocumentsOptions validation."""

import pytest

from docling.datamodel.pipeline_options_vlm_model import (
    InferenceFramework,
    ResponseFormat,
)

from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
    VlmModelApi,
    VlmModelLocal,
)


class TestPresetAndCustomConfigMutualExclusivity:
    """Test that preset and custom config cannot both be specified."""

    def test_vlm_pipeline_mutual_exclusivity(self):
        """Test VLM pipeline preset and custom config are mutually exclusive."""
        with pytest.raises(
            ValueError,
            match="Cannot specify both.*vlm_pipeline_preset.*vlm_pipeline_custom_config",
        ):
            ConvertDocumentsOptions(
                vlm_pipeline_preset="default",
                vlm_pipeline_custom_config={"engine_type": "transformers"},
            )

    def test_picture_description_mutual_exclusivity(self):
        """Test picture description preset and custom config are mutually exclusive."""
        with pytest.raises(
            ValueError,
            match="Cannot specify both.*picture_description_preset.*picture_description_custom_config",
        ):
            ConvertDocumentsOptions(
                picture_description_preset="default",
                picture_description_custom_config={"engine_type": "transformers"},
            )

    def test_code_formula_mutual_exclusivity(self):
        """Test code/formula preset and custom config are mutually exclusive."""
        with pytest.raises(
            ValueError,
            match="Cannot specify both.*code_formula_preset.*code_formula_custom_config",
        ):
            ConvertDocumentsOptions(
                code_formula_preset="default",
                code_formula_custom_config={"engine_type": "transformers"},
            )


class TestLegacyAndNewMutualExclusivity:
    """Test that legacy and new options cannot be mixed."""

    def test_vlm_legacy_and_preset(self):
        """Test that legacy VLM fields and preset cannot be mixed."""
        with pytest.raises(ValueError, match="Cannot mix legacy.*with new"):
            ConvertDocumentsOptions(
                vlm_pipeline_model_local=VlmModelLocal(
                    repo_id="test",
                    response_format=ResponseFormat.DOCTAGS,
                    inference_framework=InferenceFramework.TRANSFORMERS,
                ),
                vlm_pipeline_preset="default",
            )

    def test_vlm_legacy_and_custom_config(self):
        """Test that legacy VLM fields and custom config cannot be mixed."""
        with pytest.raises(ValueError, match="Cannot mix legacy.*with new"):
            ConvertDocumentsOptions(
                vlm_pipeline_model_api=VlmModelApi(
                    url="http://test",
                    response_format=ResponseFormat.DOCTAGS,
                ),
                vlm_pipeline_custom_config={"engine_type": "api_generic"},
            )

    def test_picture_description_legacy_and_preset(self):
        """Test that legacy picture description and preset cannot be mixed."""
        with pytest.raises(ValueError, match="Cannot mix legacy.*with new"):
            ConvertDocumentsOptions(
                picture_description_local={"repo_id": "test"},
                picture_description_preset="default",
            )

    def test_picture_description_legacy_and_custom_config(self):
        """Test that legacy picture description and custom config cannot be mixed."""
        with pytest.raises(ValueError, match="Cannot mix legacy.*with new"):
            ConvertDocumentsOptions(
                picture_description_api={"url": "http://test"},
                picture_description_custom_config={"engine_type": "api_generic"},
            )


class TestDeprecationWarnings:
    """Test that deprecation warnings are raised for legacy fields."""

    def test_vlm_pipeline_model_deprecation(self):
        """Test deprecation warning for vlm_pipeline_model."""
        with pytest.warns(DeprecationWarning, match="vlm_pipeline_model.*deprecated"):
            ConvertDocumentsOptions(
                vlm_pipeline_model="granite_docling",
            )

    def test_vlm_pipeline_model_local_deprecation(self):
        """Test deprecation warning for vlm_pipeline_model_local."""
        with pytest.warns(
            DeprecationWarning, match="vlm_pipeline_model_local.*deprecated"
        ):
            ConvertDocumentsOptions(
                vlm_pipeline_model_local=VlmModelLocal(
                    repo_id="test",
                    response_format=ResponseFormat.DOCTAGS,
                    inference_framework=InferenceFramework.TRANSFORMERS,
                ),
            )

    def test_vlm_pipeline_model_api_deprecation(self):
        """Test deprecation warning for vlm_pipeline_model_api."""
        with pytest.warns(
            DeprecationWarning, match="vlm_pipeline_model_api.*deprecated"
        ):
            ConvertDocumentsOptions(
                vlm_pipeline_model_api=VlmModelApi(
                    url="http://test",
                    response_format=ResponseFormat.DOCTAGS,
                ),
            )

    def test_picture_description_local_deprecation(self):
        """Test deprecation warning for picture_description_local."""
        with pytest.warns(
            DeprecationWarning, match="picture_description_local.*deprecated"
        ):
            ConvertDocumentsOptions(
                picture_description_local={"repo_id": "test"},
            )

    def test_picture_description_api_deprecation(self):
        """Test deprecation warning for picture_description_api."""
        with pytest.warns(
            DeprecationWarning, match="picture_description_api.*deprecated"
        ):
            ConvertDocumentsOptions(
                picture_description_api={"url": "http://test"},
            )


class TestValidConfigurations:
    """Test that valid configurations are accepted."""

    def test_preset_only(self):
        """Test that preset-only configuration is valid."""
        options = ConvertDocumentsOptions(
            vlm_pipeline_preset="default",
        )
        assert options.vlm_pipeline_preset == "default"
        assert options.vlm_pipeline_custom_config is None

    def test_custom_config_only(self):
        """Test that custom config-only configuration is valid."""
        options = ConvertDocumentsOptions(
            vlm_pipeline_custom_config={
                "engine_type": "transformers",
                "repo_id": "test",
            },
        )
        assert options.vlm_pipeline_custom_config is not None
        assert options.vlm_pipeline_preset is None

    def test_no_new_fields(self):
        """Test that configuration without new fields is valid (legacy)."""
        with pytest.warns(DeprecationWarning):
            options = ConvertDocumentsOptions(
                vlm_pipeline_model_local=VlmModelLocal(
                    repo_id="test",
                    response_format=ResponseFormat.DOCTAGS,
                    inference_framework=InferenceFramework.TRANSFORMERS,
                ),
            )
        assert options.vlm_pipeline_model_local is not None

    def test_multiple_presets(self):
        """Test that multiple preset types can be specified together."""
        options = ConvertDocumentsOptions(
            vlm_pipeline_preset="default",
            picture_description_preset="default",
            code_formula_preset="default",
        )
        assert options.vlm_pipeline_preset == "default"
        assert options.picture_description_preset == "default"
        assert options.code_formula_preset == "default"

    def test_field_serialization(self):
        """Test that fields can be serialized and deserialized."""
        options = ConvertDocumentsOptions(
            vlm_pipeline_preset="default",
            picture_description_custom_config={"engine_type": "transformers"},
        )

        # Serialize
        data = options.model_dump()
        assert data["vlm_pipeline_preset"] == "default"
        assert data["picture_description_custom_config"] is not None

        # Deserialize
        options2 = ConvertDocumentsOptions.model_validate(data)
        assert options2.vlm_pipeline_preset == "default"
        assert options2.picture_description_custom_config is not None
