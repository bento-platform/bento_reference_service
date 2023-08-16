import re
from bento_reference_service import __version__

__all__ = [
    "BENTO_SERVICE_KIND",
    "SERVICE_GROUP",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "RANGE_HEADER_PATTERN",
]

BENTO_SERVICE_KIND = "reference"

SERVICE_GROUP = "ca.c3g.bento"
SERVICE_ARTIFACT = BENTO_SERVICE_KIND

SERVICE_TYPE = {
    "group": SERVICE_GROUP,
    "artifact": SERVICE_ARTIFACT,
    "version": __version__,
}

RANGE_HEADER_PATTERN = re.compile(r"^bytes=(\d+)-(\d+)?$")
