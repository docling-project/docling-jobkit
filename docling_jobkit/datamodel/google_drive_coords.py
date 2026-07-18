# These types moved to docling.datamodel.service.sources; re-exported here for
# backward compatibility because this module path has already shipped.
from docling.datamodel.service.sources import (
    GoogleDriveCoordinates,
    GoogleDriveCredentials,
)

__all__ = ["GoogleDriveCoordinates", "GoogleDriveCredentials"]
