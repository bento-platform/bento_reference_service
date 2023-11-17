from bento_lib.workflows.models import WorkflowDefinition
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import FileResponse
from pathlib import Path

from bento_reference_service.authz import authz_middleware
from bento_reference_service.workflows.metadata import workflow_set

__all__ = ["workflow_router"]

WORKFLOWS_PATH = Path(__file__).parent / "wdls"
workflow_router = APIRouter(prefix="/genomes")


@workflow_router.get("", dependencies=[authz_middleware.dep_public_endpoint()])
def workflow_list():
    return workflow_set.workflow_dicts_by_type_and_id()


@workflow_router.get("/{workflow_id}.wdl", dependencies=[authz_middleware.dep_public_endpoint()])
def workflow_file(workflow_id: str):
    if not workflow_set.workflow_exists(workflow_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No workflow with ID {workflow_id}")
    return FileResponse(WORKFLOWS_PATH / workflow_set.get_workflow_resource(workflow_id))


@workflow_router.get("/{workflow_id}", dependencies=[authz_middleware.dep_public_endpoint()])
def workflow_item(workflow_id: str) -> WorkflowDefinition:
    if not workflow_set.workflow_exists(workflow_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No workflow with ID {workflow_id}")
    return workflow_set.get_workflow(workflow_id)
