from fastapi import FastAPI, Response

from typing import List

from . import __version__, indices, models
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es
from .genomes import make_genome_path, get_genome, get_genomes
from .utils import make_uri

app = FastAPI()

REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"


@app.on_event("startup")
async def app_startup() -> None:
    # Create all ES indices if needed
    for index in indices.ALL_INDICES:
        if not await es.indices.exists(index=index["name"]):
            await es.indices.create(index=index["name"], mappings=index["mappings"])


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


def contig_to_response(c: models.Contig) -> dict:
    return {
        **c.dict(),
        "refget": make_uri(f"/sequences/{c.trunc512}"),
    }


def genome_contigs_response(g: models.Genome) -> List[dict]:
    return [contig_to_response(c) for c in g.contigs]


def genome_to_response(g: models.Genome) -> dict:
    return {
        **g.dict(exclude={"fasta", "fai"}),
        "contigs": genome_contigs_response(g),
        "fasta": make_uri(f"/genomes/{g.id}.fa"),
        "fai": make_uri(f"/genomes/{g.id}.fa.fai"),
    }


@app.get("/genomes")
async def genomes_list() -> List[dict]:
    return [genome_to_response(g) async for g in get_genomes()]


# TODO: more normal genome creation endpoint


@app.post("/private/ingest")
async def genomes_ingest() -> List[dict]:
    # Weird endpoint for now - old Bento ingest style backwards compatibility
    pass  # TODO


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
    genome_path = make_genome_path(genome_id)
    return genome_to_response(await get_genome(genome_path))


@app.get("/genomes/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str):
    genome_path = make_genome_path(genome_id)
    return genome_contigs_response(await get_genome(genome_path))


@app.get("/genomes/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(genome_id: str, contig_name: str):
    pass


@app.get("/genomes/{genome_id}/gene_features.gtf.gz")
async def genomes_detail_gene_features(genome_id: str):
    # TODO: how to return empty gtf.gz if nothing is here yet?
    pass  # TODO: slices of GTF.gz


@app.get("/genomes/{genome_id}/gene_features.gtf.gz.tbi")
async def genomes_detail_gene_features_index(genome_id: str):
    # TODO: how to return empty gtf.gz.tbi if nothing is here yet?
    pass  # TODO: gene features GTF tabix file


# TODO: more normal annotation PUT endpoint
#  - treat gene_features as a file that can be replaced basically


# RefGet

@app.get("/sequence/{sequence_checksum}")
async def refget_sequence(response: Response, sequence_checksum: str):
    # TODO: query based on index (exact OR with checksums); fall back to iteration if needed

    # TODO: start - query arg
    # TODO: end - query arg

    # TODO: Range

    # TODO: Validate max length - subsequence_limit

    # TODO: Not Acceptable response to non-text plain (with fallbacks) request

    response.headers["Content-Type"] = REFGET_HEADER_TEXT
    # TODO: generate chunks in response


@app.get("/sequence/{sequence_checksum}/metadata")
async def refget_sequence_metadata(response: Response, sequence_checksum: str) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON

    # TODO: query based on index (exact OR with checksums); fall back to iteration if needed

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

            # I don't like that they used the word 'subsequence' here... that's not what that means exactly.
            # It's a substring!
            "subsequence_limit": config.response_substring_limit,

            "supported_api_versions": ["1.0"],
        }
    }
