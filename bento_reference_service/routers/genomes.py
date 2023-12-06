from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from .. import models as m
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..db import DatabaseDependency
from ..streaming import generate_uri_streaming_response


__all__ = ["genome_router"]


genome_router = APIRouter(prefix="/genomes")


@genome_router.get("", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_list(
    db: DatabaseDependency, response_format: str | None = None
) -> tuple[m.GenomeWithURIs, ...] | tuple[str, ...]:
    genomes = await db.get_genomes()
    if response_format == "id_list":
        return tuple(g.id for g in genomes)
    # else, format as full response
    return genomes


@genome_router.post("")
async def genomes_create(db: DatabaseDependency, genome: m.Genome, request: Request) -> m.GenomeWithURIs:
    if g := await db.create_genome(genome):
        authz_middleware.mark_authz_done(request)
        return g
    else:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not find genome with ID {genome.id} after creation",
        )


# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@genome_router.get("/{genome_id}.fa", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta(
    genome_id: str, config: ConfigDependency, db: DatabaseDependency, request: Request
) -> StreamingResponse:
    genome: m.Genome = await db.get_genome(genome_id)

    if genome is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(config, genome.fasta, range_header, "text/x-fasta")


@genome_router.get("/{genome_id}.fa.fai", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta_index(
    genome_id: str, config: ConfigDependency, db: DatabaseDependency, request: Request
) -> StreamingResponse:
    genome: m.Genome = await db.get_genome(genome_id)

    if genome is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(config, genome.fasta, range_header, "text/plain")


@genome_router.get("/{genome_id}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail(genome_id: str, db: DatabaseDependency) -> m.GenomeWithURIs:
    if g := await db.get_genome(genome_id):
        return g
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")


@genome_router.get("/{genome_id}/contigs", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_contigs(genome_id: str, db: DatabaseDependency) -> tuple[m.ContigWithRefgetURI, ...]:
    if g := await db.get_genome(genome_id):
        return g.contigs
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")


@genome_router.get("/{genome_id}/contigs/{contig_name}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_contig_detail(
    genome_id: str, contig_name: str, db: DatabaseDependency
) -> m.ContigWithRefgetURI:
    genome: m.GenomeWithURIs | None = await db.get_genome(genome_id)
    if genome is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")

    contig: m.ContigWithRefgetURI | None = next((c for c in genome.contigs if c.name == contig_name), None)

    if contig is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contig with name {contig_name} not found in genome with ID {genome_id}",
        )

    return contig


# @genome_router.get("/{genome_id}/gene_features.gtf.gz")
# async def genomes_detail_gene_features(genome_id: str):
#     # TODO: how to return empty gtf.gz if nothing is here yet?
#     raise NotImplementedError()
#     # TODO: slices of GTF.gz
#
#
# @genome_router.get("/{genome_id}/gene_features.gtf.gz.tbi")
# async def genomes_detail_gene_features_index(genome_id: str):
#     # TODO: how to return empty gtf.gz.tbi if nothing is here yet?
#     raise NotImplementedError()  # TODO: gene features GTF tabix file
#
#
# # TODO: more normal annotation PUT endpoint
# #  - treat gene_features as a file that can be replaced basically
