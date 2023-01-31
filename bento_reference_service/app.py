from fastapi import FastAPI

from . import __version__
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es, create_all_indices
from .routers.genomes import genome_router
from .routers.ingest import ingest_router
from .routers.refget import refget_router

app = FastAPI()


@app.on_event("startup")
async def app_startup() -> None:
    """
    Perform all app startup tasks, including creating all ES indices if needed.
    """

    # Create all ES indices if needed
    await create_all_indices()


@app.on_event("shutdown")
async def app_shutdown() -> None:
    """
    Perform all app pre-shutdown tasks, for now just closing the ES connection.
    """
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
        "contactUrl": config.service_contact_url,
        "version": __version__,
        "environment": "prod",
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
            "dataService": True,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }
