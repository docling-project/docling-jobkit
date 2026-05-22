import pytest
from pydantic import ValidationError

from docling_jobkit.cli.local import JobConfig as LocalJobConfig
from docling_jobkit.cli.multiproc import JobConfig as MultiprocJobConfig


@pytest.mark.parametrize("job_config_cls", [LocalJobConfig, MultiprocJobConfig])
def test_legacy_cli_job_config_rejects_presigned_target_shape(job_config_cls) -> None:
    with pytest.raises(ValidationError):
        job_config_cls.model_validate(
            {
                "sources": [],
                "target": {"kind": "presigned_url"},
            }
        )
