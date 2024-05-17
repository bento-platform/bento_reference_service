from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError, StarletteHTTPException

from bento_lib.responses.fastapi_errors import (
    http_exception_handler_factory,
    validation_exception_handler_factory,
)
from bento_lib.service_info.helpers import build_service_info_from_pydantic_config

from . import __version__
from .authz import authz_middleware
from .config import get_config, ConfigDependency
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .db import get_db
from .logger import get_logger, LoggerDependency
from .routers.genomes import genome_router
from .routers.refget import refget_router
from .routers.tasks import task_router
from .routers.workflows import workflow_router


# TODO: Find a way to DI this
config_for_setup = get_config()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db = get_db(config_for_setup, get_logger(config_for_setup))

    # If we have any tasks that are still marked as "running" on application startup, we need to move them to the error
    # state.
    await db.move_running_tasks_to_error()
    await db.close()

    yield


app = FastAPI(lifespan=lifespan)

# Attach different routers to the app, for:
# - genome listing
# - asynchronous task querying
# - our RefGet API implementation
# - our workflow metadata and WDLs
app.include_router(genome_router)
app.include_router(task_router)
app.include_router(refget_router)
app.include_router(workflow_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config_for_setup.cors_origins,
    allow_credentials=True,
    allow_headers=["Authorization", "Cache-Control"],
    allow_methods=["*"],
)

# Non-standard middleware setup so that we can import the instance and use it for dependencies too
authz_middleware.attach(app)

app.exception_handler(StarletteHTTPException)(
    http_exception_handler_factory(get_logger(config_for_setup), authz_middleware)
)
app.exception_handler(RequestValidationError)(validation_exception_handler_factory(authz_middleware))


# Create the required ingestion temporary directory if needed
config_for_setup.file_ingest_tmp_dir.mkdir(exist_ok=True)


@app.get("/service-info", dependencies=[authz_middleware.dep_public_endpoint()])
async def service_info(config: ConfigDependency, logger: LoggerDependency):
    return await build_service_info_from_pydantic_config(
        config,
        logger,
        {
            "serviceKind": BENTO_SERVICE_KIND,
            "dataService": False,
            "workflowProvider": True,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
        SERVICE_TYPE,
        __version__,
    )
