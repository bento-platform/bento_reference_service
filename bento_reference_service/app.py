from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError, StarletteHTTPException

from bento_lib.responses.fastapi_errors import (
    http_exception_handler_factory,
    validation_exception_handler_factory,
)

from . import __version__
from .authz import authz_middleware
from .config import get_config, ConfigDependency
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .logger import get_logger
from .routers.data_types import data_type_router
from .routers.genomes import genome_router
from .routers.refget import refget_router
from .routers.schemas import schema_router


app = FastAPI()

# Attach different routers to the app, for:
# - data type listing (for Bento-style search)
# - genome listing
# - our RefGet API implementation
# - our JSON schemas
app.include_router(data_type_router)
app.include_router(genome_router)
app.include_router(refget_router)
app.include_router(schema_router)

# TODO: Find a way to DI this
config_for_setup = get_config()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config_for_setup.cors_origins,
    allow_headers=["Authorization"],
    allow_credentials=True,
    allow_methods=["*"],
)

# Non-standard middleware setup so that we can import the instance and use it for dependencies too
authz_middleware.attach(app)

app.exception_handler(StarletteHTTPException)(
    http_exception_handler_factory(get_logger(config_for_setup), authz_middleware)
)
app.exception_handler(RequestValidationError)(validation_exception_handler_factory(authz_middleware))


@app.get("/service-info")
async def service_info(config: ConfigDependency):
    return {
        "id": config.service_id,
        "name": config.service_name,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Reference data (genomes & annotations) service for the Bento platform.",
        "organization": {"name": "C3G", "url": "https://www.computationalgenomics.ca"},
        "contactUrl": config.service_contact_url,
        "version": __version__,
        "environment": "prod",
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
            "dataService": False,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }
