import aiofiles

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

from typing import List, Optional

from bento_reference_service import models
from bento_reference_service.constants import RANGE_HEADER_PATTERN
from bento_reference_service.genomes import get_genomes
from bento_reference_service.utils import make_uri, get_genome_or_error


__all__ = ["genome_router"]


def exc_bad_range(range_header: str) -> HTTPException:
    return HTTPException(status_code=400, detail=f"invalid range header value: {range_header}")


CHUNK_SIZE = 1024 * 16  # 16 KB at a time

genome_router = APIRouter(prefix="/genomes")


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


@genome_router.get("/genomes")
async def genomes_list() -> List[dict]:
    return [genome_to_response(g) async for g in get_genomes()]


# TODO: more normal genome creation endpoint

# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@genome_router.get("/genomes/{genome_id}.fa")
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


@genome_router.get("/genomes/{genome_id}.fa.fai")
async def genomes_detail_fasta_index(genome_id: str):
    genome: models.Genome = await get_genome_or_error(genome_id)
    return FileResponse(genome.fai, filename=f"{genome_id}.fa.fai")


@genome_router.get("/genomes/{genome_id}")
async def genomes_detail(genome_id: str):
    return genome_to_response(await get_genome_or_error(genome_id))


@genome_router.get("/genomes/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str):
    return genome_contigs_response(await get_genome_or_error(genome_id))


@genome_router.get("/genomes/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(genome_id: str, contig_name: str):
    # TODO: Use ES in front?
    genome: models.Genome = await get_genome_or_error(genome_id)
    raise NotImplementedError()


@genome_router.get("/genomes/{genome_id}/gene_features.gtf.gz")
async def genomes_detail_gene_features(genome_id: str):
    # TODO: how to return empty gtf.gz if nothing is here yet?
    raise NotImplementedError()
    # TODO: slices of GTF.gz


@genome_router.get("/genomes/{genome_id}/gene_features.gtf.gz.tbi")
async def genomes_detail_gene_features_index(genome_id: str):
    # TODO: how to return empty gtf.gz.tbi if nothing is here yet?
    raise NotImplementedError()  # TODO: gene features GTF tabix file


# TODO: more normal annotation PUT endpoint
#  - treat gene_features as a file that can be replaced basically
