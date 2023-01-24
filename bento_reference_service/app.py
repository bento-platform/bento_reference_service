from fastapi import FastAPI

from . import __version__
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE

app = FastAPI()


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
async def genomes_list():
    pass


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
