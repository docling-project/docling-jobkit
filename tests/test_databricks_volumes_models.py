import pytest
from pydantic import ValidationError

from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
    TaskDatabricksVolumesSource,
)


def _coords(**overrides) -> dict:
    base = {
        "workspace_host": "dbc-xxxxxxx.cloud.databricks.com",
        "token": "tok",
        "volume_path": "/Volumes/main/default/docs",
    }
    base.update(overrides)
    return base


def test_accepts_valid_coordinates():
    coords = DatabricksVolumesCoordinates(**_coords())

    assert coords.workspace_host == "dbc-xxxxxxx.cloud.databricks.com"
    assert coords.token.get_secret_value() == "tok"
    assert coords.volume_path == "/Volumes/main/default/docs"
    assert coords.max_num_elements is None


def test_rejects_workspace_host_with_scheme():
    with pytest.raises(ValidationError, match="bare hostname"):
        DatabricksVolumesCoordinates(
            **_coords(workspace_host="https://dbc-xxxxxxx.cloud.databricks.com")
        )


def test_rejects_volume_path_without_prefix():
    with pytest.raises(ValidationError, match="/Volumes/"):
        DatabricksVolumesCoordinates(**_coords(volume_path="/main/default/docs"))


def test_strips_trailing_slash_from_volume_path():
    coords = DatabricksVolumesCoordinates(
        **_coords(volume_path="/Volumes/main/default/docs/")
    )

    assert coords.volume_path == "/Volumes/main/default/docs"


def test_task_source_carries_kind_discriminator():
    source = TaskDatabricksVolumesSource(**_coords())

    assert source.kind == "databricks_volumes"
