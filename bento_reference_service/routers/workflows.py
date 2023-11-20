from bento_lib.workflows.fastapi import build_workflow_router

from bento_reference_service.authz import authz_middleware
from bento_reference_service.workflows.metadata import workflow_set

__all__ = ["workflow_router"]

workflow_router = build_workflow_router(authz_middleware, workflow_set)
