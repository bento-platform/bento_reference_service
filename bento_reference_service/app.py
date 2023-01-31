import aiofiles
import re

import pysam
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, StreamingResponse

from typing import List, Optional, Union

from . import __version__, indices, models
from .config import config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .es import es, create_all_indices
from .genomes import make_genome_path, get_genome, get_genomes
from .logger import logger
from .utils import make_uri

app = FastAPI()

REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"

RANGE_HEADER_PATTERN = re.compile(r"^bytes=(\d+)-(\d+)?$")


def exc_bad_range(range_header: str) -> HTTPException:
    return HTTPException(status_code=400, detail=f"invalid range header value: {range_header}")


CHUNK_SIZE = 1024 * 16  # 16 KB at a time


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
    raise NotImplementedError()  # TODO


# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


async def get_genome_or_error(genome_id: str) -> models.Genome:
    genome_path = make_genome_path(genome_id)

    if not genome_path.exists():
        raise HTTPException(status_code=404, detail=f"genome not found: {genome_id}")

    # TODO: handle format errors with 500
    return await get_genome(genome_path)


@app.get("/genomes/{genome_id}.fa")
async def genomes_detail_fasta(genome_id: str, request: Request):
    genome: models.Genome = await get_genome_or_error(genome_id)

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: Optional[str] = request.headers.get("Range", None)

    if range_header is None:
        # TODO: send the file if no range header and the FASTA is below some response size limit
        raise NotImplementedError()

    range_header_match = RANGE_HEADER_PATTERN.match(range_header)
    if not range_header_match:
        raise exc_bad_range(range_header)

    start: int = 0
    end: Optional[int] = None

    try:
        start = int(range_header_match.group(1))
        end_val = range_header_match.group(2)
        end = end_val if end_val is None else int(end_val)
    except ValueError:
        raise exc_bad_range(range_header)

    async def stream_file():
        # TODO: Use range support from FastAPI when it is merged
        async with aiofiles.open(genome.fasta, "rb") as ff:
            # Logic mostly ported from bento_drs

            # First, skip over <start> bytes to get to the beginning of the range
            ff.seek(start)

            byte_offset: int = start
            while True:
                # Add a 1 to the amount to read if it's below chunk size, because the last coordinate is inclusive.
                data = ff.read(min(CHUNK_SIZE, (end + 1 - byte_offset) if end is not None else CHUNK_SIZE))
                byte_offset += len(data)
                yield data

                # If we've hit the end of the file and are reading empty byte strings, or we've reached the
                # end of our range (inclusive), then escape the loop.
                # This is guaranteed to terminate with a finite-sized file.
                if not data or (end is not None and byte_offset > end):
                    break

    return StreamingResponse(stream_file(), media_type="text/x-fasta", status_code=206 if range_header else 200)


@app.get("/genomes/{genome_id}.fa.fai")
async def genomes_detail_fasta_index(genome_id: str):
    genome: models.Genome = await get_genome_or_error(genome_id)
    return FileResponse(genome.fai, filename=f"{genome_id}.fa.fai")


@app.get("/genomes/{genome_id}")
async def genomes_detail(genome_id: str):
    return genome_to_response(await get_genome_or_error(genome_id))


@app.get("/genomes/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str):
    return genome_contigs_response(await get_genome_or_error(genome_id))


@app.get("/genomes/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(genome_id: str, contig_name: str):
    # TODO: Use ES in front?
    genome: models.Genome = await get_genome_or_error(genome_id)
    raise NotImplementedError()


@app.get("/genomes/{genome_id}/gene_features.gtf.gz")
async def genomes_detail_gene_features(genome_id: str):
    # TODO: how to return empty gtf.gz if nothing is here yet?
    raise NotImplementedError()
    # TODO: slices of GTF.gz


@app.get("/genomes/{genome_id}/gene_features.gtf.gz.tbi")
async def genomes_detail_gene_features_index(genome_id: str):
    # TODO: how to return empty gtf.gz.tbi if nothing is here yet?
    raise NotImplementedError()  # TODO: gene features GTF tabix file


# TODO: more normal annotation PUT endpoint
#  - treat gene_features as a file that can be replaced basically


# RefGet

async def get_contig_by_checksum(checksum: str) -> Optional[models.Contig]:
    es_resp = await es.search(index=indices.genome_index["name"], query={
        "nested": {
            "path": "contigs",
            "query": {
                "bool": {
                    "should": [
                        {"match": {"contigs.md5": checksum}},
                        {"match": {"contigs.trunc512": checksum}},
                    ],
                },
            },
            "inner_hits": {},
            "score_mode": "max",  # We are interested in the
        },
    })

    if es_resp["hits"]["total"]["value"] > 0:
        # Use ES result, since we got a hit
        sg = es_resp["hits"]["hits"][0]

        # We have the genome, but we still need to extract the contig from inner hits
        if sg["inner_hits"]["contigs"]["hits"]["total"]["value"] > 0:
            # We have a contig hit, so return it
            return models.Contig(**sg["inner_hits"]["contigs"]["hits"]["hits"][0]["_source"])

        for sc in sg["contigs"]:
            if checksum in (sc["md5"], sc["trunc512"]):
                return models.Contig(**sc)

        logger.error(f"Found ES hit for checksum {checksum} but could not find contig in inner hits")

    logger.debug(f"No hits in ES index for checksum {checksum}")

    # Manually iterate as a fallback
    async for genome in get_genomes():
        for sc in genome.contigs:
            if checksum in (sc.md5, sc.trunc512):
                logger.warning(f"Found manual hit for {checksum}, but no corresponding entry in ES index")
                return sc

    return None


@app.get("/sequence/{sequence_checksum}")
async def refget_sequence(
    request: Request,
    response: Response,
    sequence_checksum: str,
    start: Union[int, None] = None,
    end: Union[int, None] = None,
):
    response.headers["Content-Type"] = REFGET_HEADER_TEXT_WITH_CHARSET

    accept_header: Optional[str] = request.headers.get("Accept", None)
    if accept_header and accept_header not in (REFGET_HEADER_TEXT_WITH_CHARSET, REFGET_HEADER_TEXT, "text/plain"):
        raise HTTPException(status_code=406, detail="Not Acceptable")   # TODO: plain text error

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: Optional[str] = request.headers.get("Range", None)

    if (start or end) and range_header:
        # TODO: Valid plain text error
        raise HTTPException(status_code=400, detail="cannot specify both start/end and Range header")

    contig: Optional[models.Contig] = await get_contig_by_checksum(sequence_checksum)

    start_final: int = 0  # 0-based, inclusive
    end_final: int = contig.length - 1  # 0-based, exclusive - need to adjust range (which is inclusive)

    if start is not None:
        if end is not None:
            response.headers["Accept-Ranges"] = "none"
            if start > end:
                if not contig.circular:
                    raise HTTPException(status_code=416, detail="Range Not Satisfiable")
            end_final = end
        start_final = start

    if range_header is not None:
        range_header_match = RANGE_HEADER_PATTERN.match(range_header)
        if not range_header_match:
            raise HTTPException(status_code=400, detail="bad range")

        try:
            start_final = int(range_header_match.group(1))
            if end_val := range_header_match.group(2):
                end_final = end_val + 1  # range is inclusive, so we have to adjust it to be exclusive
        except ValueError:
            raise HTTPException(status_code=400, detail="bad range")

    # Final bounds-checking
    if start_final >= contig.length:
        # start is 0-based; so if it's set to contig.length or more, it is out of range.
        raise HTTPException(status_code=400, detail="start cannot be longer than sequence")
    if end_final > contig.length:
        # end is 0-based inclusive
        raise HTTPException(status_code=400, detail="end cannot be past the end of the sequence")

    if contig is None:
        # TODO: proper 404 for refget spec
        raise HTTPException(status_code=404, detail=f"sequence not found with checksum: {sequence_checksum}")

    if end_final - start_final > config.response_substring_limit:
        raise HTTPException(status_code=400, detail="request for too many bytes")  # TODO: what is real error?

    genome = await get_genome_or_error(contig.genome)

    fa = pysam.FastaFile(filename=str(genome.fasta), filepath_index=str(genome.fai))
    try:
        # TODO: handle missing region / coordinate exceptions explicitly
        return fa.fetch(contig.name, start_final, end_final).encode("ascii")
    finally:
        fa.close()


@app.get("/sequence/{sequence_checksum}/metadata")
async def refget_sequence_metadata(response: Response, sequence_checksum: str) -> dict:  # TODO: type: refget resp
    contig: Optional[models.Contig] = await get_contig_by_checksum(sequence_checksum)

    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET

    if contig is None:
        # TODO: proper 404 for refget spec
        # TODO: proper content type for exception - RefGet error class?
        raise HTTPException(status_code=404, detail=f"sequence not found with checksum: {sequence_checksum}")

    return {
        "metadata": {
            "md5": contig.md5,
            "trunc512": contig.trunc512,
            "length": contig.length,
            "aliases": [a.dict() for a in contig.aliases],
        },
    }


@app.get("/sequence/service-info")
async def refget_service_info(response: Response) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET
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
