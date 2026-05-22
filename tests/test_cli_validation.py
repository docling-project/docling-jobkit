import pytest

from docling.datamodel.service.targets import PresignedUrlTarget

from docling_jobkit.cli.local import JobConfig as LocalJobConfig
from docling_jobkit.cli.multiproc import JobConfig as MultiprocJobConfig


@pytest.mark.parametrize("job_config_cls", [LocalJobConfig, MultiprocJobConfig])
def test_legacy_cli_job_config_accepts_presigned_target_shape(job_config_cls) -> None:
    config = job_config_cls.model_validate(
        {
            "sources": [],
            "target": {"kind": "presigned_url"},
        }
    )

    assert isinstance(config.target, PresignedUrlTarget)
