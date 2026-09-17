import pytest
from pydantic import ValidationError

from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
    DatabricksVolumesSourceCoordinates,
    TaskDatabricksVolumesSource,
    TaskDatabricksVolumesTarget,
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


def test_source_coordinates_carry_max_num_elements():
    coords = DatabricksVolumesSourceCoordinates(**_coords())

    assert coords.max_num_elements is None
    assert (
        DatabricksVolumesSourceCoordinates(
            **_coords(max_num_elements=5)
        ).max_num_elements
        == 5
    )


def test_target_has_no_max_num_elements():
    """max_num_elements caps source enumeration and means nothing on a target."""
    assert "max_num_elements" not in TaskDatabricksVolumesTarget.model_fields
    assert "max_num_elements" in TaskDatabricksVolumesSource.model_fields
