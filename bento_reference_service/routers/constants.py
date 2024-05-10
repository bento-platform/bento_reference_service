from bento_lib.auth.permissions import P_INGEST_REFERENCE_MATERIAL
from bento_lib.auth.resources import RESOURCE_EVERYTHING

from ..authz import authz_middleware

__all__ = ["DEPENDENCY_INGEST_REFERENCE_MATERIAL"]


DEPENDENCY_INGEST_REFERENCE_MATERIAL = authz_middleware.dep_require_permissions_on_resource(
    frozenset({P_INGEST_REFERENCE_MATERIAL}), RESOURCE_EVERYTHING
)
