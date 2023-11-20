import aiofiles

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse, StreamingResponse

from bento_reference_service import models as m
from bento_reference_service.config import ConfigDependency
from bento_reference_service.constants import RANGE_HEADER_PATTERN
from bento_reference_service.genomes import get_genome_or_error, get_genome_with_uris_or_error, get_genomes_with_uris
from bento_reference_service.logger import LoggerDependency


__all__ = ["genome_router"]


def exc_bad_range(range_header: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid range header value: {range_header}")


genome_router = APIRouter(prefix="/genomes")


@genome_router.get("")
async def genomes_list(config: ConfigDependency, logger: LoggerDependency) -> list[m.GenomeWithURIs]:
    return [g async for g in get_genomes_with_uris(config, logger)]


# TODO: more normal genome creation endpoint

# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@genome_router.get("/{genome_id}.fa")
async def genomes_detail_fasta(genome_id: str, config: ConfigDependency, request: Request) -> StreamingResponse:
    genome: m.Genome = await get_genome_or_error(genome_id, config)

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)

    if range_header is None:
        # TODO: send the file if no range header and the FASTA is below some response size limit
        raise NotImplementedError()

    range_header_match = RANGE_HEADER_PATTERN.match(range_header)
    if not range_header_match:
        raise exc_bad_range(range_header)

    start: int = 0
    end: int | None = None

    try:
        start = int(range_header_match.group(1))
        end_val = range_header_match.group(2)
        end = end_val if end_val is None else int(end_val)
    except ValueError:
        raise exc_bad_range(range_header)

    async def stream_file():
        chunk_size = config.file_response_chunk_size

        # TODO: Use range support from FastAPI when it is merged
        async with aiofiles.open(genome.fasta, "rb") as ff:
            # Logic mostly ported from bento_drs

            # First, skip over <start> bytes to get to the beginning of the range
            await ff.seek(start)

            byte_offset: int = start
            while True:
                # Add a 1 to the amount to read if it's below chunk size, because the last coordinate is inclusive.
                data = await ff.read(
                    min(
                        chunk_size,
                        (end + 1 - byte_offset) if end is not None else chunk_size,
                    )
                )
                byte_offset += len(data)
                yield data

                # If we've hit the end of the file and are reading empty byte strings, or we've reached the
                # end of our range (inclusive), then escape the loop.
                # This is guaranteed to terminate with a finite-sized file.
                if not data or (end is not None and byte_offset > end):
                    break

    return StreamingResponse(
        stream_file(),
        media_type="text/x-fasta",
        status_code=status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK,
    )


@genome_router.get("/{genome_id}.fa.fai")
async def genomes_detail_fasta_index(genome_id: str, config: ConfigDependency) -> FileResponse:
    genome: m.Genome = await get_genome_or_error(genome_id, config)
    return FileResponse(genome.fai, filename=f"{genome_id}.fa.fai")


@genome_router.get("/{genome_id}")
async def genomes_detail(genome_id: str, config: ConfigDependency) -> m.GenomeWithURIs:
    return await get_genome_with_uris_or_error(genome_id, config)


@genome_router.get("/{genome_id}/contigs")
async def genomes_detail_contigs(genome_id: str, config: ConfigDependency) -> list[m.ContigWithRefgetURI]:
    return (await get_genome_with_uris_or_error(genome_id, config)).contigs


@genome_router.get("/{genome_id}/contigs/{contig_name}")
async def genomes_detail_contig_detail(
    genome_id: str, contig_name: str, config: ConfigDependency
) -> m.ContigWithRefgetURI:
    # TODO: Use ES in front?

    genome: m.GenomeWithURIs = await get_genome_with_uris_or_error(genome_id, config)
    contig: m.ContigWithRefgetURI | None = next((c for c in genome.contigs if c.name == contig_name), None)

    if contig is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contig with name {contig_name} not found in genome with ID {genome_id}",
        )

    return contig


@genome_router.get("/{genome_id}/gene_features.gtf.gz")
async def genomes_detail_gene_features(genome_id: str):
    # TODO: how to return empty gtf.gz if nothing is here yet?
    raise NotImplementedError()
    # TODO: slices of GTF.gz


@genome_router.get("/{genome_id}/gene_features.gtf.gz.tbi")
async def genomes_detail_gene_features_index(genome_id: str):
    # TODO: how to return empty gtf.gz.tbi if nothing is here yet?
    raise NotImplementedError()  # TODO: gene features GTF tabix file


# TODO: more normal annotation PUT endpoint
#  - treat gene_features as a file that can be replaced basically
