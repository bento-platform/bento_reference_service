from fastapi import FastAPI
from config import Config

from . import __version__
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE

app = FastAPI()
config = Config()


@app.get("/service-info")
async def service_info():
    return {
        "id": config.service_id,
        "name": config.service_name,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Reference data (genomes &amp; annotations) service for the Bento platform.",
        "organization": {
            "name": "C3G",
            "url": "https://www.computationalgenomics.ca"
        },
        "contactUrl": "mailto:info@c3g.ca",
        "version": __version__,
        "environment": "prod",
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }
