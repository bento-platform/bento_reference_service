from fastapi import APIRouter, HTTPException, status

from ..db import DatabaseDependency
from ..models import Task
from .constants import DEPENDENCY_INGEST_REFERENCE_MATERIAL

__all__ = ["task_router"]


task_router = APIRouter(prefix="/tasks")


@task_router.get("", dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL])
async def tasks_list(db: DatabaseDependency):
    return await db.query_tasks(None, None)


@task_router.get("/{task_id}", dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL])
async def tasks_detail(task_id: int, db: DatabaseDependency) -> Task:
    task = await db.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task with ID {task_id} not found")
    return task
