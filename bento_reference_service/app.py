from fastapi import FastAPI

from . import __version__
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es, create_all_indices
from .routers.genomes import genome_router
from .routers.ingest import ingest_router
from .routers.refget import refget_router

app = FastAPI()

REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"


@app.on_event("startup")
async def app_startup() -> None:
    """
    Perform all app startup tasks, including creating all ES indices if needed.
    """
    await create_all_indices()


@app.on_event("shutdown")
async def app_shutdown() -> None:
    # Don't 'leak' ES connection - close it when the app process is closed
    await es.close()


# Attach different routers to the app - for genome listing, ingest handling, and our RefGet API implementation
app.include_router(genome_router)
app.include_router(ingest_router)
app.include_router(refget_router)


@app.get("/service-info")
async def service_info():
    return {
        "id": config.service_id,
        "name": config.service_name,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Reference data (genomes & annotations) service for the Bento platform.",
        "organization": {
            "name": "C3G",
            "url": "https://www.computationalgenomics.ca"
        },
        "contactUrl": "mailto:info@c3g.ca",
        "version": __version__,
        "environment": "prod",
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
            "dataService": True,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }

