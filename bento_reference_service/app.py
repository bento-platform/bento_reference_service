from fastapi import FastAPI

from typing import List

from . import __version__, indices
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es
from .genomes import get_genomes
from .utils import make_uri

app = FastAPI()


@app.on_event("startup")
async def app_startup() -> None:
    # Create ES indices based on service ID if needed

    if not await es.indices.exists(index=indices.gene_feature_index_name):
        await es.indices.create(index=indices.gene_feature_index_name, mappings=indices.gene_feature_mappings)


@app.on_event("shutdown")
async def app_shutdown() -> None:
    # Don't 'leak' ES connection - close it when the app process is closed
    await es.close()


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
            "gitRepository": "https://github.com/bento-platform/bento_reference_service",
        },
    }


@app.get("/genomes")
async def genomes_list() -> List[dict]:
    return [
        {
            **g.dict(exclude={"fasta", "fai"}),
            "fasta": make_uri(f"/genomes/{g.id}.fa"),
            "fai": make_uri(f"/genomes/{g.id}.fa.fai"),
        }
        async for g in get_genomes()
    ]


@app.get("/genomes/{genome_id}.fa")
async def genomes_detail_fasta(genome_id: str):
    pass


@app.get("/genomes/{genome_id}.fa.fai")
async def genomes_detail_fasta_index(genome_id: str):
    pass


@app.get("/genomes/{genome_id}")
async def genomes_detail(genome_id: str):
    pass


@app.get("/genomes/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str):
    pass


@app.get("/genomes/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(genome_id: str, contig_name: str):
    pass
