import subprocess
import sys

import pytest

from docling.datamodel.service.sources import AzureBlobCoordinates

from docling_jobkit.config.target_config import AzurePresignedConfig
from docling_jobkit.orchestrators.local.orchestrator import LocalOrchestratorConfig
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestratorConfig


@pytest.mark.parametrize(
    "config_type",
    [LocalOrchestratorConfig, RQOrchestratorConfig, RayOrchestratorConfig],
)
def test_orchestrator_config_round_trips_azure_presigned_config(config_type):
    presigned_config = AzurePresignedConfig(
        azure_coords=AzureBlobCoordinates(
            account_name="acct",
            container="artifacts",
            connection_string="AccountName=acct;AccountKey=dGVzdA==",
        )
    )
    config = config_type(presigned_config=presigned_config)

    restored = config_type.model_validate(config.model_dump(mode="json"))

    assert isinstance(restored.presigned_config, AzurePresignedConfig)
    assert restored.presigned_config.azure_coords.container == "artifacts"


def test_result_modules_import_without_azure_extra():
    script = """
import importlib.abc
import sys

class BlockAzure(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "azure" or fullname.startswith("azure."):
            raise ModuleNotFoundError(fullname)
        return None

sys.meta_path.insert(0, BlockAzure())
import docling_jobkit.config
import docling_jobkit.convert.results
"""

    subprocess.run([sys.executable, "-c", script], check=True)
