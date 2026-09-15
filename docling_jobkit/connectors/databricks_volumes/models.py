from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator


class DatabricksVolumesCoordinates(BaseModel):
    """Connection details shared by the source and target connectors."""

    workspace_host: Annotated[
        str,
        Field(
            description="Bare Databricks workspace hostname (no scheme).",
            examples=["dbc-xxxxxxx.cloud.databricks.com"],
        ),
    ]

    token: Annotated[
        SecretStr,
        Field(description="Databricks personal access token (PAT)."),
    ]

    volume_path: Annotated[
        str,
        Field(
            description=(
                "Absolute Unity Catalog volume path to read from. May point "
                "at the volume root or any subdirectory within it; "
                "subdirectories are traversed recursively."
            ),
            examples=[
                "/Volumes/main/default/docs",
                "/Volumes/main/default/docs/inbox",
            ],
        ),
    ]

    @field_validator("workspace_host")
    @classmethod
    def _no_scheme(cls, v: str) -> str:
        if "://" in v:
            raise ValueError("workspace_host must be a bare hostname, no scheme")
        return v

    @field_validator("volume_path")
    @classmethod
    def _absolute_volumes_path(cls, v: str) -> str:
        if not v.startswith("/Volumes/"):
            raise ValueError("volume_path must start with '/Volumes/'")
        return v.rstrip("/")


class DatabricksVolumesSourceCoordinates(DatabricksVolumesCoordinates):
    """Source-only coordinates.

    ``max_num_elements`` caps enumeration and is meaningless on a target, so it
    lives here rather than on the shared base — the same split the other
    connectors make between their source and target coordinate models.
    """

    max_num_elements: Annotated[
        Optional[int],
        Field(description="Optional cap on the number of files processed."),
    ] = None


class TaskDatabricksVolumesSource(DatabricksVolumesSourceCoordinates):
    kind: Literal["databricks_volumes"] = "databricks_volumes"


class TaskDatabricksVolumesTarget(DatabricksVolumesCoordinates):
    kind: Literal["databricks_volumes"] = "databricks_volumes"


__all__ = [
    "DatabricksVolumesCoordinates",
    "DatabricksVolumesSourceCoordinates",
    "TaskDatabricksVolumesSource",
    "TaskDatabricksVolumesTarget",
]
