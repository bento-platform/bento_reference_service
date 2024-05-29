from bento_lib.service_info.constants import SERVICE_GROUP_BENTO
from bento_lib.service_info.helpers import build_bento_service_type
from bento_reference_service import __version__

__all__ = [
    "BENTO_SERVICE_KIND",
    "SERVICE_GROUP",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
]

BENTO_SERVICE_KIND = "reference"

SERVICE_GROUP = SERVICE_GROUP_BENTO
SERVICE_ARTIFACT = BENTO_SERVICE_KIND

SERVICE_TYPE = build_bento_service_type(SERVICE_ARTIFACT, __version__)
