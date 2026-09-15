"""Unit tests for DoclingConverterManager preset and engine control."""

import pytest

from docling.datamodel.picture_classification_options import (
    DocumentPictureClassifierOptions,
)
from docling.datamodel.pipeline_options import (
    CodeFormulaVlmOptions,
    PictureDescriptionVlmEngineOptions,
)

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
