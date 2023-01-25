from fastapi import FastAPI, Response

from typing import List

from . import __version__, indices, models
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es
from .genomes import get_genome, get_genomes
from .utils import make_uri

app = FastAPI()

REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"


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


def genome_to_response(g: models.Genome) -> dict:
    return {
        **g.dict(exclude={"fasta", "fai"}),
        "fasta": make_uri(f"/genomes/{g.id}.fa"),
        "fai": make_uri(f"/genomes/{g.id}.fa.fai"),
    }


def contig_to_response(c: models.Contig) -> dict:
    return {
        **c.dict(),
        "refget": make_uri(f"/sequences/{c.trunc512}"),
    }


@app.get("/genomes")
async def genomes_list() -> List[dict]:
    return [genome_to_response(g) async for g in get_genomes()]


# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@app.get("/genomes/{genome_id}.fa")
async def genomes_detail_fasta(genome_id: str):
    pass


@app.get("/genomes/{genome_id}.fa.fai")
async def genomes_detail_fasta_index(genome_id: str):
    pass


@app.get("/genomes/{genome_id}")
async def genomes_detail(genome_id: str):
    genome_path = config.data_path / f"{genome_id}.bentoGenome"

    # Make sure the genome path is correctly nested inside the data directory
    # TODO

    return genome_to_response(await get_genome(genome_path))


@app.get("/genomes/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str):
    pass


@app.get("/genomes/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(genome_id: str, contig_name: str):
    pass


# RefGet

@app.get("/sequence/{sequence_checksum}")
async def refget_sequence(response: Response, sequence_checksum: str):
    response.headers["Content-Type"] = REFGET_HEADER_TEXT

    # TODO: start
    # TODO: end

    # TODO: Range
    # TODO: Not Acceptable response to non-text plain (with fallbacks) request

    pass


@app.get("/sequence/{sequence_checksum}/metadata")
async def refget_sequence_metadata(response: Response, sequence_checksum: str) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON

    # TODO: get contig - maybe we need to index them...

    return {
        "metadata": {
            # TODO
            "md5": "TODO",
            "trunc512": "TODO",
            "length": "TODO",
            "aliases": [],  # TODO
        },
    }


@app.get("/sequence/service-info")
async def refget_service_info(response: Response) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON

    return {
        "service": {
            "circular_supported": False,
            "algorithms": ["md5", "trunc512"],
            "subsequence_limit": 10000,  # TODO
            "supported_api_versions": ["1.0"],
        }
    }
