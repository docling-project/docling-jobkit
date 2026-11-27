"""Unit tests for DoclingConverterManager preset and engine control."""

import pytest

from docling.datamodel.base_models import InputFormat
from docling.datamodel.chart_extraction_options import ChartExtractionVlmEngineOptions
from docling.datamodel.picture_classification_options import (
    DocumentPictureClassifierOptions,
)
from docling.datamodel.pipeline_options import (
    AsrPipelineOptions,
    CodeFormulaVlmOptions,
    PictureDescriptionVlmEngineOptions,
)
from docling.datamodel.pipeline_options_asr_model import (
    ApiAsrOptions,
    InlineAsrNativeWhisperOptions,
    InlineAsrOptions,
    InlineAsrTransformersOptions,
)
from docling.pipeline.vlm_pipeline import VlmPipeline

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
    InlineAsrMlxAudioOptions,
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

    def test_default_vlm_preset_is_reachable_by_its_own_id(self):
        """The configured default must also be selectable by name.

        Every other registry excludes the alias "default" from its loop, so that the
        alias entry survives. The VLM loop excluded default_vlm_preset instead, which
        dropped a real Docling preset id -- and with the stock config that id is
        "granite_docling", so the shipped default was the one preset a request could
        not name.
        """
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
        )
        manager = DoclingConverterManager(config)

        assert "granite_docling" in manager.vlm_preset_registry
        # ...and the alias still resolves to it rather than being overwritten.
        assert manager.vlm_preset_registry["default"]["preset_id"] == "granite_docling"

        by_id = manager._parse_vlm_options(
            ConvertDocumentsOptions(vlm_pipeline_preset="granite_docling")
        )
        by_alias = manager._parse_vlm_options(
            ConvertDocumentsOptions(vlm_pipeline_preset="default")
        )
        assert by_id == by_alias

    def test_an_explicit_vlm_allowlist_still_restricts(self):
        """Registering the default by name must not widen an explicit allowlist."""
        config = DoclingConverterManagerConfig(
            default_vlm_preset="granite_docling",
            allowed_vlm_presets=["smoldocling"],
        )
        manager = DoclingConverterManager(config)

        assert sorted(manager.vlm_preset_registry) == ["default", "smoldocling"]
        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_vlm_options(
                ConvertDocumentsOptions(vlm_pipeline_preset="qwen")
            )

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

    def test_chart_extraction_registry(self):
        """Test chart extraction preset registry."""
        config = DoclingConverterManagerConfig(
            default_chart_extraction_preset="granite_vision_v4",
        )
        manager = DoclingConverterManager(config)

        assert "default" in manager.chart_extraction_preset_registry
        assert (
            manager.chart_extraction_preset_registry["default"]["preset_id"]
            == "granite_vision_v4"
        )


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

    def test_invalid_chart_extraction_preset_rejected(self):
        """Test that invalid chart extraction preset raises error."""
        config = DoclingConverterManagerConfig(
            allowed_chart_extraction_presets=["granite_vision_v4"],
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_preset(
                "nonexistent",
                manager.chart_extraction_preset_registry,
                "Chart extraction",
            )


class TestPipelineControl:
    """Test the default and allowed pipeline configuration."""

    @pytest.mark.parametrize(
        "kwargs",
        [{"allowed_pipelines": ["standard", "asr"]}, {"default_pipeline": "legacy"}],
    )
    def test_unsupported_pipeline_rejected(self, kwargs):
        with pytest.raises(ValueError, match="not supported"):
            DoclingConverterManagerConfig(**kwargs)

    def test_default_pipeline_must_be_allowed(self):
        with pytest.raises(
            ValueError, match="default_pipeline 'standard' is not in allowed_pipelines"
        ):
            DoclingConverterManagerConfig(allowed_pipelines=["vlm"])

    @pytest.mark.skipif(
        ConvertDocumentsOptions.model_fields["pipeline"].default is not None,
        reason="requires a docling version where `pipeline` can be omitted",
    )
    def test_omitted_pipeline_uses_default(self):
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(
                default_pipeline="vlm", allowed_pipelines=["vlm"]
            )
        )

        pdf_format_option = manager.get_pdf_pipeline_opts(ConvertDocumentsOptions())

        assert pdf_format_option.pipeline_cls is VlmPipeline

    def test_disallowed_pipeline_rejected(self):
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(
                default_pipeline="vlm", allowed_pipelines=["vlm"]
            )
        )

        with pytest.raises(ValueError, match="Pipeline 'standard' is not allowed"):
            manager.get_pdf_pipeline_opts(ConvertDocumentsOptions(pipeline="standard"))


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

    @pytest.mark.parametrize(
        ("config_type", "config_field"),
        [
            ("table_structure", "allow_custom_table_structure_config"),
            ("layout", "allow_custom_layout_config"),
            (
                "picture_classification",
                "allow_custom_picture_classification_config",
            ),
            ("ocr", "allow_custom_ocr_config"),
            ("chart_extraction", "allow_custom_chart_extraction_config"),
            ("asr", "allow_custom_asr_config"),
        ],
    )
    def test_stage_custom_config_not_allowed(self, config_type, config_field):
        """Test that stage custom configs are rejected when not allowed."""
        config = DoclingConverterManagerConfig(**{config_field: False})
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._validate_custom_config_allowed(config_type)

    @pytest.mark.parametrize(
        ("config_type", "config_field"),
        [
            ("table_structure", "allow_custom_table_structure_config"),
            ("layout", "allow_custom_layout_config"),
            (
                "picture_classification",
                "allow_custom_picture_classification_config",
            ),
            ("ocr", "allow_custom_ocr_config"),
            ("chart_extraction", "allow_custom_chart_extraction_config"),
            ("asr", "allow_custom_asr_config"),
        ],
    )
    def test_stage_custom_config_allowed(self, config_type, config_field):
        """Test that stage custom configs are accepted when allowed."""
        config = DoclingConverterManagerConfig(**{config_field: True})
        manager = DoclingConverterManager(config)

        manager._validate_custom_config_allowed(config_type)


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

    def test_parse_code_formula_options_with_preset(self):
        """Test parsing code/formula options from preset."""
        config = DoclingConverterManagerConfig(
            default_code_formula_preset="codeformulav2",
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            code_formula_preset="default",
        )

        options = manager._parse_code_formula_options(request)
        assert options is not None

    @pytest.mark.parametrize(
        "request_field, parser_name",
        [
            ("vlm_pipeline_preset", "_parse_vlm_options"),
            ("picture_description_preset", "_parse_picture_description_options"),
            ("code_formula_preset", "_parse_code_formula_options"),
            ("chart_extraction_preset", "_parse_chart_extraction_options"),
            ("table_structure_preset", "_parse_table_structure_options"),
            ("layout_preset", "_parse_layout_options"),
            ("ocr_preset", "_parse_ocr_options"),
        ],
    )
    def test_default_alias_resolves_with_stock_config(self, request_field, parser_name):
        """Every default_*_preset must name a real Docling preset.

        The registries map the alias "default" onto the configured
        default_<stage>_preset, so a stock config has to be able to resolve
        "default" for every stage. Picture classification is left out because
        its docling branch deliberately returns None until the factory exists.
        """
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        request = ConvertDocumentsOptions(**{request_field: "default"})

        assert getattr(manager, parser_name)(request) is not None


def _raw_engine_options() -> dict:
    return {
        "engine_type": "api",
        "url": "http://localhost:8000/v1/chat/completions",
        "params": {"model": "some-model"},
    }


def _raw_model_spec() -> dict:
    return {
        "name": "some-model",
        "default_repo_id": "some-model",
        "prompt": "Describe this image.",
        "response_format": "plaintext",
    }


class TestCustomPresetByNameOptionsParsing:
    """Custom presets registered by name arrive as raw dicts and must be
    validated into their options model, not returned as-is (#191)."""

    def test_picture_description_custom_preset_by_name(self):
        """Custom picture_description preset resolves to a validated options object."""
        from docling.datamodel.pipeline_options import (
            PictureDescriptionVlmEngineOptions,
        )

        config = DoclingConverterManagerConfig(
            custom_picture_description_presets={
                "my_preset": {
                    "picture_area_threshold": 0.0,
                    "engine_options": _raw_engine_options(),
                    "model_spec": _raw_model_spec(),
                    "prompt": "Describe this image.",
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(picture_description_preset="my_preset")
        options = manager._parse_picture_description_options(request)

        assert isinstance(options, PictureDescriptionVlmEngineOptions)

    def test_picture_description_custom_preset_area_threshold_assignable(self):
        """Reproduces #191: assigning picture_area_threshold must not raise
        AttributeError on a dict."""
        config = DoclingConverterManagerConfig(
            default_picture_description_preset="my_preset",
            custom_picture_description_presets={
                "my_preset": {
                    "picture_area_threshold": 0.0,
                    "engine_options": _raw_engine_options(),
                    "model_spec": _raw_model_spec(),
                    "prompt": "Describe this image.",
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            picture_description_preset="my_preset",
            picture_description_area_threshold=0.2,
        )
        pipeline_options = manager._parse_standard_pdf_opts(request, None)

        assert (
            pipeline_options.picture_description_options.picture_area_threshold == 0.2
        )

    def test_code_formula_custom_preset_by_name(self):
        """Custom code_formula preset resolves to a validated options object."""
        from docling.datamodel.pipeline_options import CodeFormulaVlmOptions

        config = DoclingConverterManagerConfig(
            custom_code_formula_presets={
                "my_preset": {
                    "engine_options": _raw_engine_options(),
                    "model_spec": _raw_model_spec(),
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(code_formula_preset="my_preset")
        options = manager._parse_code_formula_options(request)

        assert isinstance(options, CodeFormulaVlmOptions)

    def test_vlm_custom_preset_by_name(self):
        """Non-regression: VLM custom preset by name already worked and must keep working."""
        from docling.datamodel.pipeline_options import VlmConvertOptions

        config = DoclingConverterManagerConfig(
            custom_vlm_presets={
                "my_preset": {
                    "engine_options": _raw_engine_options(),
                    "model_spec": {**_raw_model_spec(), "response_format": "doctags"},
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(vlm_pipeline_preset="my_preset")
        options = manager._parse_vlm_options(request)

        assert isinstance(options, VlmConvertOptions)

    def test_picture_description_custom_preset_engine_not_allowed(self):
        """Engine allowlist must still apply once the dict is validated."""
        config = DoclingConverterManagerConfig(
            allowed_picture_description_engines=["mlx"],
            custom_picture_description_presets={
                "my_preset": {
                    "engine_options": _raw_engine_options(),
                    "model_spec": _raw_model_spec(),
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(picture_description_preset="my_preset")
        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_picture_description_options(request)

    def test_picture_description_custom_config_non_regression(self):
        """picture_description_custom_config (not by-name preset) must keep working."""
        from docling.datamodel.pipeline_options import (
            PictureDescriptionVlmEngineOptions,
        )

        config = DoclingConverterManagerConfig(
            allow_custom_picture_description_config=True,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            picture_description_custom_config={
                "engine_options": _raw_engine_options(),
                "model_spec": _raw_model_spec(),
                "prompt": "Describe this image.",
            }
        )
        options = manager._parse_picture_description_options(request)

        assert isinstance(options, PictureDescriptionVlmEngineOptions)


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

    def test_parse_table_structure_custom_config_not_allowed(self):
        """Test that custom table structure config raises error when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_table_structure_config=False,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            table_structure_custom_config={
                "kind": "docling_tableformer",
                "mode": "fast",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_table_structure_options(request)

    def test_parse_layout_custom_config_not_allowed(self):
        """Test that custom layout config raises error when not allowed."""
        config = DoclingConverterManagerConfig(allow_custom_layout_config=False)
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            layout_custom_config={
                "kind": "docling_layout_default",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_layout_options(request)

    def test_parse_picture_classification_custom_config_not_allowed(self):
        """Test that custom picture classification config raises error when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_picture_classification_config=False,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            picture_classification_custom_config={
                "kind": "document_picture_classifier",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_picture_classification_options(request)

    def test_parse_ocr_custom_config_not_allowed(self):
        """Test that custom OCR config raises error when not allowed."""
        config = DoclingConverterManagerConfig(allow_custom_ocr_config=False)
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            ocr_custom_config={
                "kind": "auto",
            },
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_ocr_options(request)


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


class TestBuiltInPresetsAllowedByDefault:
    """`allowed_*_presets=None` is documented as "None means all are allowed".

    The VLM, OCR, chunking and table-structure registries honour that: with no
    allow-list they enumerate every built-in preset. Picture-description,
    code/formula and picture-classification instead only ever registered the
    ``default`` alias, so every real Docling preset id was rejected with
    "Allowed presets: default" -- the opposite of the documented contract.
    """

    @pytest.mark.parametrize(
        "registry_name, expected_ids",
        [
            (
                "picture_description_preset_registry",
                PictureDescriptionVlmEngineOptions.list_preset_ids(),
            ),
            (
                "code_formula_preset_registry",
                CodeFormulaVlmOptions.list_preset_ids(),
            ),
            (
                "picture_classification_preset_registry",
                DocumentPictureClassifierOptions.list_preset_ids(),
            ),
            (
                "chart_extraction_preset_registry",
                ChartExtractionVlmEngineOptions.list_preset_ids(),
            ),
        ],
    )
    def test_all_built_in_presets_registered_without_allow_list(
        self, registry_name, expected_ids
    ):
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        registry = getattr(manager, registry_name)

        assert set(registry) == {"default", *expected_ids}

    @pytest.mark.parametrize(
        "config_field, registry_name, allowed",
        [
            (
                "allowed_picture_description_presets",
                "picture_description_preset_registry",
                ["smolvlm"],
            ),
            (
                "allowed_code_formula_presets",
                "code_formula_preset_registry",
                ["granite_docling"],
            ),
            (
                "allowed_picture_classification_presets",
                "picture_classification_preset_registry",
                ["document_figure_classifier_v2"],
            ),
            (
                "allowed_chart_extraction_presets",
                "chart_extraction_preset_registry",
                ["granite_vision_v4"],
            ),
        ],
    )
    def test_allow_list_still_restricts(self, config_field, registry_name, allowed):
        """An explicit allow-list must keep excluding everything else."""
        config = DoclingConverterManagerConfig(**{config_field: allowed})
        manager = DoclingConverterManager(config)

        assert set(getattr(manager, registry_name)) == {"default", *allowed}

    def test_layout_registry_is_kind_based_and_unchanged(self):
        """Layout presets are factory *kinds*, not stage preset ids.

        `_parse_layout_options` resolves a docling-source layout preset with
        `layout_factory.create_options(kind=preset_id)`, so enumerating
        `LayoutObjectDetectionOptions.list_preset_ids()` here would register ids
        the factory cannot build. Layout is deliberately left alone.
        """
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        assert set(manager.layout_preset_registry) == {"default"}


class TestChartExtraction:
    """Full-coverage tests for the chart extraction stage."""

    def test_parse_chart_extraction_options_with_preset(self):
        """Parsing chart extraction options from a named preset returns options."""
        config = DoclingConverterManagerConfig(
            default_chart_extraction_preset="granite_vision_v4",
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(chart_extraction_preset="default")

        options = manager._parse_chart_extraction_options(request)
        assert options is not None
        assert isinstance(options, ChartExtractionVlmEngineOptions)

    def test_parse_chart_extraction_options_without_preset_returns_none(self):
        """When no preset or custom config is given, returns None."""
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        request = ConvertDocumentsOptions()

        options = manager._parse_chart_extraction_options(request)
        assert options is None

    def test_chart_extraction_custom_preset_by_name(self):
        """A custom chart extraction preset registered by name is validated."""
        config = DoclingConverterManagerConfig(
            custom_chart_extraction_presets={
                "my_chart_preset": {
                    "engine_options": {
                        "engine_type": "api",
                        "url": "http://localhost:8000/v1/chat/completions",
                        "params": {"model": "granite-vision"},
                    },
                    "model_spec": {
                        "name": "granite-vision",
                        "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                        "prompt": "<chart2csv>",
                        "response_format": "plaintext",
                    },
                    "chart2csv": True,
                }
            },
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(chart_extraction_preset="my_chart_preset")
        options = manager._parse_chart_extraction_options(request)

        assert isinstance(options, ChartExtractionVlmEngineOptions)

    def test_chart_extraction_custom_preset_as_default(self):
        """When default_chart_extraction_preset is a custom preset, 'default' resolves to custom API options."""
        config = DoclingConverterManagerConfig(
            default_chart_extraction_preset="my_kserve_preset",
            allowed_chart_extraction_presets=["my_kserve_preset"],
            allowed_chart_extraction_engines=["api"],
            custom_chart_extraction_presets={
                "my_kserve_preset": {
                    "engine_options": {
                        "engine_type": "api",
                        "url": "http://vlm-inference:8080/v1/chat/completions",
                        "params": {"model": "granite-vision-4-1-4b"},
                    },
                    "model_spec": {
                        "name": "granite-vision-4-1-4b",
                        "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                        "prompt": "<chart2csv>",
                        "response_format": "plaintext",
                    },
                }
            },
        )
        manager = DoclingConverterManager(config)

        # 1. Request with do_chart_extraction=True without setting chart_extraction_preset explicitly
        request = ConvertDocumentsOptions(do_chart_extraction=True)
        options = manager._parse_chart_extraction_options(request)

        assert isinstance(options, ChartExtractionVlmEngineOptions)
        assert options.engine_options.engine_type.value == "api"

        # 2. Standard PDF pipeline options wiring
        pdf_format_opts = manager.get_pdf_pipeline_opts(request)
        pipeline_opts = pdf_format_opts.pipeline_options
        assert pipeline_opts.do_chart_extraction is True
        assert (
            pipeline_opts.chart_extraction_options.engine_options.engine_type.value
            == "api"
        )

    def test_chart_extraction_custom_config_not_allowed(self):
        """Custom chart extraction config is rejected when not allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_chart_extraction_config=False,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            chart_extraction_custom_config={
                "engine_options": {
                    "engine_type": "api",
                    "url": "http://localhost:8000/v1/chat/completions",
                    "params": {"model": "granite-vision"},
                },
                "model_spec": {
                    "name": "granite-vision",
                    "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                    "prompt": "<chart2csv>",
                    "response_format": "plaintext",
                },
            }
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_chart_extraction_options(request)

    def test_chart_extraction_custom_config_allowed(self):
        """Custom chart extraction config is accepted when allowed."""
        config = DoclingConverterManagerConfig(
            allow_custom_chart_extraction_config=True,
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            chart_extraction_custom_config={
                "engine_options": {
                    "engine_type": "api",
                    "url": "http://localhost:8000/v1/chat/completions",
                    "params": {"model": "granite-vision"},
                },
                "model_spec": {
                    "name": "granite-vision",
                    "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                    "prompt": "<chart2csv>",
                    "response_format": "plaintext",
                },
            }
        )

        options = manager._parse_chart_extraction_options(request)
        assert isinstance(options, ChartExtractionVlmEngineOptions)

    def test_chart_extraction_engine_not_allowed(self):
        """Engine allowlist is enforced for chart extraction custom config."""
        config = DoclingConverterManagerConfig(
            allow_custom_chart_extraction_config=True,
            allowed_chart_extraction_engines=["mlx"],
        )
        manager = DoclingConverterManager(config)

        request = ConvertDocumentsOptions(
            chart_extraction_custom_config={
                "engine_options": {
                    "engine_type": "transformers",
                },
                "model_spec": {
                    "name": "granite-vision",
                    "default_repo_id": "ibm-granite/granite-vision-4.1-4b",
                    "prompt": "<chart2csv>",
                    "response_format": "plaintext",
                },
            }
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager._parse_chart_extraction_options(request)

    def test_chart_extraction_wired_into_standard_pipeline(self):
        """Chart extraction options are forwarded to PdfPipelineOptions."""
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        request = ConvertDocumentsOptions(
            do_chart_extraction=True,
            chart_extraction_preset="default",
        )
        pdf_format_option = manager.get_pdf_pipeline_opts(request)
        pipeline_opts = pdf_format_option.pipeline_options

        assert pipeline_opts.do_chart_extraction is True
        assert isinstance(
            pipeline_opts.chart_extraction_options, ChartExtractionVlmEngineOptions
        )


def _raw_api_asr_options() -> dict:
    return {
        "kind": "api_model_options",
        "url": "http://localhost:8000/v1/audio/transcriptions",
        "model": "whisper-large-v3-turbo",
    }


_HAS_ASR_REQUEST_FIELDS = "asr_preset" in ConvertDocumentsOptions.model_fields
_HAS_ASR_REGISTRY = hasattr(InlineAsrOptions, "from_preset")
requires_asr_request_fields = pytest.mark.skipif(
    not (_HAS_ASR_REQUEST_FIELDS and _HAS_ASR_REGISTRY),
    reason="needs a Docling release with the ASR preset registry and request fields",
)


def _audio_asr_options(manager: DoclingConverterManager, request) -> object:
    converter = manager.get_converter(manager.get_converter_options(request))
    return converter.format_to_options[InputFormat.AUDIO].pipeline_options.asr_options


class TestAsrPresets:
    """ASR presets: server-side registry, request selection and the cache key."""

    def test_default_preset_is_docling_default(self):
        """Without configuration, "default" is Docling's default ASR options."""
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        assert manager.asr_preset_registry["default"]["source"] == "docling"
        assert manager._default_asr_options == AsrPipelineOptions().asr_options
        assert manager._parse_asr_options(ConvertDocumentsOptions()) is None

    def test_custom_inline_preset(self):
        """A raw inline preset is validated into its InlineAsrOptions subclass."""
        config = DoclingConverterManagerConfig(
            custom_asr_presets={
                "whisper_turbo": {
                    "kind": "inline_model_options",
                    "inference_framework": "whisper",
                    "repo_id": "turbo",
                }
            },
        )
        manager = DoclingConverterManager(config)

        options = manager._resolve_asr_preset("whisper_turbo")

        assert isinstance(options, InlineAsrNativeWhisperOptions)
        assert options.repo_id == "turbo"
        assert manager._default_asr_options == AsrPipelineOptions().asr_options

    @pytest.mark.skipif(
        InlineAsrMlxAudioOptions is None, reason="needs mlx-audio ASR options"
    )
    def test_custom_mlx_audio_preset(self):
        config = DoclingConverterManagerConfig(
            custom_asr_presets={
                "parakeet_mlx": {
                    "kind": "inline_model_options",
                    "inference_framework": "mlx_audio",
                    "repo_id": "mlx-community/parakeet-ctc-1.1b",
                }
            },
        )
        manager = DoclingConverterManager(config)

        options = manager._resolve_asr_preset("parakeet_mlx")

        assert isinstance(options, InlineAsrMlxAudioOptions)

    def test_custom_remote_preset_as_default(self):
        """When default_asr_preset is a custom preset, "default" resolves to it."""
        config = DoclingConverterManagerConfig(
            default_asr_preset="remote_whisper",
            custom_asr_presets={"remote_whisper": _raw_api_asr_options()},
        )
        manager = DoclingConverterManager(config)

        options = manager._default_asr_options

        assert isinstance(options, ApiAsrOptions)
        assert str(options.url) == "http://localhost:8000/v1/audio/transcriptions"
        assert options.model == "whisper-large-v3-turbo"
        assert manager._resolve_asr_preset("remote_whisper") == options

    def test_disallowed_preset_rejected(self):
        config = DoclingConverterManagerConfig(
            allowed_asr_presets=[],
            custom_asr_presets={"remote_whisper": _raw_api_asr_options()},
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="not allowed"):
            manager._resolve_asr_preset("whisper_large")

    @pytest.mark.skipif(_HAS_ASR_REGISTRY, reason="Docling has ASR presets")
    def test_allowed_preset_without_docling_registry_rejected(self):
        """Without Docling's ASR registry, an allowed id must be custom."""
        config = DoclingConverterManagerConfig(allowed_asr_presets=["whisper_large"])
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="no built-in ASR presets"):
            manager._resolve_asr_preset("whisper_large")

    def test_raw_preset_without_kind_rejected(self):
        raw_options = _raw_api_asr_options()
        del raw_options["kind"]
        config = DoclingConverterManagerConfig(
            custom_asr_presets={"remote_whisper": raw_options},
        )
        manager = DoclingConverterManager(config)

        with pytest.raises(ValueError, match="'kind'"):
            manager._resolve_asr_preset("remote_whisper")

    @pytest.mark.parametrize(
        "config",
        [
            {"default_asr_preset": "no_such_preset", "allowed_asr_presets": []},
            {
                "default_asr_preset": "x",
                "custom_asr_presets": {"x": {"url": "http://h/v1"}},
            },
        ],
    )
    def test_wrong_default_preset_fails_at_startup(self, config):
        """A wrong default ASR preset stops the manager from being built."""
        with pytest.raises(ValueError, match="Invalid default ASR preset"):
            DoclingConverterManager(DoclingConverterManagerConfig(**config))

    def test_unused_wrong_custom_preset_does_not_break_pdf(self):
        """A bad custom preset that is not the default only fails when chosen."""
        raw_options = _raw_api_asr_options()
        del raw_options["kind"]
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(custom_asr_presets={"bad": raw_options})
        )

        converter = manager.get_converter(
            manager.get_converter_options(ConvertDocumentsOptions())
        )

        assert converter is not None

    @requires_asr_request_fields
    def test_request_selects_docling_preset(self):
        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        options = _audio_asr_options(
            manager, ConvertDocumentsOptions(asr_preset="whisper_tiny_transformers")
        )

        assert isinstance(options, InlineAsrTransformersOptions)
        assert options.repo_id == "openai/whisper-tiny"

    @requires_asr_request_fields
    def test_request_selects_custom_preset(self):
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(
                custom_asr_presets={"remote_whisper": _raw_api_asr_options()}
            )
        )

        options = _audio_asr_options(
            manager, ConvertDocumentsOptions(asr_preset="remote_whisper")
        )

        assert isinstance(options, ApiAsrOptions)
        assert options.model == "whisper-large-v3-turbo"

    @requires_asr_request_fields
    def test_request_preset_outside_allow_list_rejected(self):
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(allowed_asr_presets=["whisper_tiny"])
        )

        with pytest.raises(ValueError, match="not allowed"):
            manager.get_converter_options(
                ConvertDocumentsOptions(asr_preset="whisper_turbo")
            )

    @requires_asr_request_fields
    def test_request_custom_config_needs_permission(self):
        request = ConvertDocumentsOptions(asr_custom_config=_raw_api_asr_options())

        with pytest.raises(ValueError, match="Custom ASR configuration is not allowed"):
            DoclingConverterManager(
                DoclingConverterManagerConfig()
            ).get_converter_options(request)

        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(allow_custom_asr_config=True)
        )
        options = _audio_asr_options(manager, request)
        assert isinstance(options, ApiAsrOptions)
        assert str(options.url) == "http://localhost:8000/v1/audio/transcriptions"

    @requires_asr_request_fields
    def test_asr_options_are_part_of_the_converter_cache_key(self):
        manager = DoclingConverterManager(
            DoclingConverterManagerConfig(
                custom_asr_presets={"remote_whisper": _raw_api_asr_options()}
            )
        )

        def converter(**fields):
            return manager.get_converter(
                manager.get_converter_options(ConvertDocumentsOptions(**fields))
            )

        default = converter()
        assert converter() is default
        assert converter(asr_preset="default") is default
        remote = converter(asr_preset="remote_whisper")
        assert remote is not default
        assert converter(asr_preset="remote_whisper") is remote
        tiny = converter(asr_preset="whisper_tiny_transformers")
        assert tiny is not default and tiny is not remote


class TestConvertDocumentsSetupFailureTagging:
    """convert_documents() tags data-independent setup failures by type.

    Any failure in the eager setup span (option resolution through
    initialize_pipeline) must surface as PipelineInitializationError so lifecycle
    owners classify it as request-wide, never as a per-document failure. The
    subsequent convert_all() is lazy, so nothing document-specific runs here.
    """

    def test_pre_init_setup_failure_is_tagged(self, monkeypatch):
        from docling_jobkit.public_errors import PipelineInitializationError

        manager = DoclingConverterManager(DoclingConverterManagerConfig())
        # A pre-init step (option resolution) blowing up -- e.g. an unavailable
        # OCR engine -- must be tagged, not leak as a bare exception.
        monkeypatch.setattr(
            manager,
            "get_pdf_pipeline_opts",
            lambda options: (_ for _ in ()).throw(RuntimeError("OCR engine missing")),
        )
        with pytest.raises(PipelineInitializationError):
            list(
                manager.convert_documents(sources=[], options=ConvertDocumentsOptions())
            )

    def test_pipeline_init_failure_is_tagged(self, monkeypatch):
        from docling_jobkit.public_errors import PipelineInitializationError

        manager = DoclingConverterManager(DoclingConverterManagerConfig())

        class _BadConverter:
            def initialize_pipeline(self, *a, **k):
                raise FileNotFoundError("Model 'x' not found in artifacts_path.")

        monkeypatch.setattr(manager, "parse_chunking_options", lambda options: None)
        monkeypatch.setattr(manager, "get_pdf_pipeline_opts", lambda options: object())
        monkeypatch.setattr(manager, "get_converter", lambda opts: _BadConverter())
        with pytest.raises(PipelineInitializationError):
            list(
                manager.convert_documents(sources=[], options=ConvertDocumentsOptions())
            )
