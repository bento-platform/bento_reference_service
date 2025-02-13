from bento_lib.apps.fastapi import BentoFastAPI
from contextlib import asynccontextmanager
from fastapi import FastAPI

from . import __version__
from .authz import authz_middleware
from .config import get_config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .db import get_db
from .logger import get_logger
from .routers.genomes import genome_router
from .routers.refget import refget_router
from .routers.tasks import task_router
from .routers.workflows import workflow_router


BENTO_SERVICE_INFO = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": True,
    "gitRepository": "https://github.com/bento-platform/bento_reference_service",
}


# TODO: Find a way to DI this
config_for_setup = get_config()
logger_for_setup = get_logger(config_for_setup)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db = get_db(config_for_setup, logger_for_setup)

    # If we have any tasks that are still marked as "running" on application startup, we need to move them to the error
    # state.
    await db.move_running_tasks_to_error()
    await db.close()

    yield


app = BentoFastAPI(
    authz_middleware,
    config_for_setup,
    logger_for_setup,
    BENTO_SERVICE_INFO,
    SERVICE_TYPE,
    __version__,
    configure_structlog_access_logger=True,  # Set up custom access log middleware to replace the default Uvicorn one
    lifespan=lifespan,
)

# Attach different routers to the app, for:
# - genome listing
# - asynchronous task querying
# - our RefGet API implementation
# - our workflow metadata and WDLs
app.include_router(genome_router)
app.include_router(task_router)
app.include_router(refget_router)
app.include_router(workflow_router)


# Create the required ingestion temporary directory if needed
config_for_setup.file_ingest_tmp_dir.mkdir(exist_ok=True)
