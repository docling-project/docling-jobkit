"""Validation rules on the shared SharePoint/OneDrive connection coordinates.

Pure pydantic — no office365 SDK involved, so these run without the extra installed.
"""

import pytest
from pydantic import SecretStr, ValidationError

from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointSourceCoordinates,
    SharePointTargetCoordinates,
)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({}, "Exactly one"),  # neither
        ({"site_url": "u", "onedrive_user": "a@x.com"}, "Exactly one"),  # both
        ({"onedrive_user": "a@x.com", "document_library": "D"}, "document_library"),
    ],
    ids=["no_target", "both_targets", "library_with_onedrive"],
)
def test_coords_reject_ambiguous_drive_targeting(kwargs, match):
    with pytest.raises(ValidationError, match=match):
        SharePointSourceCoordinates(
            tenant="t", client_id="c", client_secret=SecretStr("s"), **kwargs
        )


def test_target_coords_omit_read_only_fields():
    """The source/target split is the reason SharePointConnection exists.

    Subclassing the wrong base would silently expose read-side knobs on the target's
    public config schema.
    """
    fields = SharePointTargetCoordinates.model_fields
    assert "file_ids" not in fields
    assert "max_num_elements" not in fields
