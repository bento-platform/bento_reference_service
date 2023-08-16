from fastapi import FastAPI

from . import __version__
from .config import ConfigDependency
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
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


@app.get("/service-info")
async def service_info(config: ConfigDependency):
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
            "dataService": False,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }
