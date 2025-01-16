import structlog
import time

from bento_lib.apps.fastapi import BentoFastAPI
from bento_lib.responses.errors import internal_server_error
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from uvicorn.protocols.utils import get_path_with_query_string

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
    lifespan=lifespan,
)


# Set up custom access log middleware to replace the default Uvicorn one
#  - This way, we can structure the access data in a better way.
# Adapted from https://gist.github.com/nymous/f138c7f06062b7c43c060bf03759c29e
# Licensed under the terms of the MIT license, (c) Thomas Gaudin
@app.middleware("http")
async def access_log_middleware(request: Request, call_next) -> Response:
    start_time = time.perf_counter_ns()

    service_logger = structlog.stdlib.get_logger(f"{BENTO_SERVICE_KIND}.logger")
    response = JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=internal_server_error(logger=service_logger)
    )
    try:
        response = await call_next(request)
    except Exception as e:
        service_logger.exception("Uncaught exception", exc_info=e)
    finally:
        duration = time.perf_counter_ns() - start_time

        status_code = response.status_code
        url = get_path_with_query_string(request.scope)
        client_host = request.client.host
        client_port = request.client.port
        http_method = request.method
        http_version = request.scope["http_version"]

        access_logger = structlog.stdlib.get_logger(f"{BENTO_SERVICE_KIND}.access")
        await access_logger.ainfo(
            # The message format mirrors the original uvicorn access message, but with response duration added.
            f'{client_host}:{client_port} - "{http_method} {url} HTTP/{http_version}" {status_code} '
            f"({duration / 10e9:.4f}s)",
            http={
                "url": url,
                "status_code": status_code,
                "method": http_method,
                "version": http_version,
            },
            network={"client": {"host": client_host, "port": client_port}},
            duration=duration,
        )

        return response


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
