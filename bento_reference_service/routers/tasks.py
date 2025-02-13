from fastapi import APIRouter, BackgroundTasks, HTTPException, status

from ..config import ConfigDependency
from ..db import DatabaseDependency
from ..drs import DrsResolverDependency
from ..features import ingest_features_task
from ..logger import LoggerDependency
from ..models import TaskParams, Task
from .constants import DEPENDENCY_INGEST_REFERENCE_MATERIAL

__all__ = ["task_router"]


task_router = APIRouter(prefix="/tasks")


@task_router.get("", dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL])
async def tasks_list(db: DatabaseDependency):
    return await db.query_tasks(None, None)


@task_router.post("", status_code=status.HTTP_201_CREATED, dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL])
async def tasks_create(
    task: TaskParams,
    background_tasks: BackgroundTasks,
    config: ConfigDependency,
    db: DatabaseDependency,
    drs_resolver: DrsResolverDependency,
    logger: LoggerDependency,
) -> Task:
    genome_id = task.genome_id

    g = await db.get_genome(genome_id)
    if g is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Genome with ID {genome_id} not found.")

    task_id = await db.create_task(genome_id, task.kind)
    task = await db.get_task(task_id)
    if task is None:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Something went wrong when creating the task."
        )

    if task.kind == "ingest_features":
        # currently, ingest_features is the only task type, so we don't need an if-statement to decide which task to
        # dispatch.
        background_tasks.add_task(ingest_features_task, genome_id, task_id, config, db, drs_resolver, logger)
    else:  # pragma: no cover
        raise NotImplementedError()

    return task


@task_router.get("/{task_id}", dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL])
async def tasks_detail(task_id: int, db: DatabaseDependency) -> Task:
    task = await db.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task with ID {task_id} not found")
    return task
