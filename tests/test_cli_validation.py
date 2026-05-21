import pytest
import typer

from docling.datamodel.service.targets import PresignedUrlTarget

from docling_jobkit.cli.local import JobConfig as LocalJobConfig
from docling_jobkit.cli.multiproc import JobConfig as MultiprocJobConfig
from docling_jobkit.cli.validation import ensure_legacy_target_supported


@pytest.mark.parametrize("job_config_cls", [LocalJobConfig, MultiprocJobConfig])
def test_legacy_cli_job_config_accepts_presigned_target_shape(job_config_cls) -> None:
    config = job_config_cls.model_validate(
        {
            "sources": [],
            "target": {"kind": "presigned_url"},
        }
    )

    assert isinstance(config.target, PresignedUrlTarget)


def test_legacy_cli_rejects_presigned_target_with_explicit_message() -> None:
    with pytest.raises(typer.BadParameter, match="do not support `presigned_url`"):
        ensure_legacy_target_supported(PresignedUrlTarget())
