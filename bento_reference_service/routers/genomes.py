import asyncpg
import traceback

from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from typing import Annotated

from .. import models as m
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..db import Database, DatabaseDependency
from ..drs import DrsResolverDependency
from ..logger import LoggerDependency
from ..streaming import generate_uri_streaming_response
from .constants import DEPENDENCY_DELETE_REFERENCE_MATERIAL, DEPENDENCY_INGEST_REFERENCE_MATERIAL


__all__ = ["genome_router"]


genome_router = APIRouter(prefix="/genomes")


async def get_genome_or_raise_404(
    db: Database, genome_id: str, external_resource_uris: bool = True
) -> m.GenomeWithURIs:
    genome: m.GenomeWithURIs = await db.get_genome(genome_id, external_resource_uris=external_resource_uris)
    if genome is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} not found")
    return genome


@genome_router.get("", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_list(
    db: DatabaseDependency,
    ids: Annotated[list[str] | None, Query()] = None,
    taxon_id: str | None = None,
    response_format: str | None = None,
) -> tuple[m.GenomeWithURIs, ...] | tuple[str, ...]:
    genomes = await db.get_genomes(ids, taxon_id, external_resource_uris=True)
    if response_format == "id_list":
        return tuple(g.id for g in genomes)
    # else, format as full response
    return genomes


@genome_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL],
)
async def genomes_create(
    db: DatabaseDependency, genome: m.Genome, logger: LoggerDependency, request: Request
) -> m.GenomeWithURIs:
    try:
        if g := await db.create_genome(genome, return_external_resource_uris=True):
            authz_middleware.mark_authz_done(request)
            return g
        else:  # pragma: no cover
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not find genome with ID {genome.id} after creation",
            )
    except asyncpg.exceptions.UniqueViolationError as e:
        await logger.aerror(f"UniqueViolationError encountered during genome creation: {e} {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Genome with ID {genome.id} already exists",
        )


# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@genome_router.get("/{genome_id}.fa", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta(
    genome_id: str,
    config: ConfigDependency,
    db: DatabaseDependency,
    drs_resolver: DrsResolverDependency,
    logger: LoggerDependency,
    request: Request,
) -> StreamingResponse:
    # need internal FASTA URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id, external_resource_uris=False)

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
        drs_resolver,
        logger,
        genome.fasta,
        range_header,
        "text/x-fasta",
        impose_response_limit=False,
        support_byte_ranges=True,
    )


@genome_router.get("/{genome_id}.fa.fai", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta_index(
    genome_id: str,
    config: ConfigDependency,
    db: DatabaseDependency,
    drs_resolver: DrsResolverDependency,
    logger: LoggerDependency,
    request: Request,
) -> StreamingResponse:
    # need internal FAI URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id, external_resource_uris=False)

    # Don't use FastAPI's auto-Header tool for the Range header 'cause I don't want to shadow Python's range() function:
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
        drs_resolver,
        logger,
        genome.fai,
        range_header,
        "text/plain",
        impose_response_limit=False,
        support_byte_ranges=True,
    )


@genome_router.get("/{genome_id}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail(genome_id: str, db: DatabaseDependency) -> m.GenomeWithURIs:
    return await get_genome_or_raise_404(db, genome_id)


@genome_router.patch(
    "/{genome_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL]
)
async def genomes_patch(genome_id: str, genome_patch: m.GenomeGFF3Patch, db: DatabaseDependency):
    await get_genome_or_raise_404(db, genome_id)
    await db.update_genome(genome_id, genome_patch)


@genome_router.delete(
    "/{genome_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[DEPENDENCY_DELETE_REFERENCE_MATERIAL],
)
async def genomes_delete(genome_id: str, db: DatabaseDependency):
    # TODO: also delete DRS objects!!

    await get_genome_or_raise_404(db, genome_id)
    await db.delete_genome(genome_id)


@genome_router.get("/{genome_id}/contigs", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_contigs(genome_id: str, db: DatabaseDependency) -> tuple[m.ContigWithRefgetURI, ...]:
    return (await get_genome_or_raise_404(db, genome_id)).contigs


@genome_router.get("/{genome_id}/contigs/{contig_name}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_contig_detail(
    genome_id: str, contig_name: str, db: DatabaseDependency
) -> m.ContigWithRefgetURI:
    genome: m.GenomeWithURIs = await get_genome_or_raise_404(db, genome_id)

    contig: m.ContigWithRefgetURI | None = next((c for c in genome.contigs if c.name == contig_name), None)
    if contig is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contig with name {contig_name} not found in genome with ID {genome_id}",
        )

    return contig


@genome_router.get("/{genome_id}/feature_types", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_feature_types(db: DatabaseDependency, genome_id: str) -> dict[str, int]:
    return await db.genome_feature_types_summary(genome_id)


@genome_router.get("/{genome_id}/features", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_features(
    db: DatabaseDependency,
    genome_id: str,
    q: str | None = None,
    q_fzy: bool = False,
    name: str | None = None,
    name_fzy: bool = False,
    position: str | None = None,
    start: int | None = None,
    end: int | None = None,
    feature_type: Annotated[list[str] | None, Query()] = None,
    offset: int = 0,
    limit: int = 10,
):
    await get_genome_or_raise_404(db, genome_id)

    st = datetime.now()

    results, pagination = await db.query_genome_features(
        genome_id, q, q_fzy, name, name_fzy, position, start, end, feature_type, offset, limit
    )

    return {
        "results": results,
        "pagination": pagination,
        "time": (datetime.now() - st).total_seconds(),
    }


@genome_router.delete(
    "/{genome_id}/features",
    dependencies=[DEPENDENCY_DELETE_REFERENCE_MATERIAL],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def genomes_detail_features_delete(db: DatabaseDependency, genome_id: str):
    await get_genome_or_raise_404(db, genome_id)
    await db.clear_genome_features(genome_id)


@genome_router.get("/{genome_id}/features/{feature_id}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_features_detail(db: DatabaseDependency, genome_id: str, feature_id: str):
    await get_genome_or_raise_404(db, genome_id)

    if feature := await db.get_genome_feature_by_id(genome_id, feature_id):
        return feature

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"Feature with ID {feature_id} not found on genome {genome_id}"
    )


@genome_router.get("/{genome_id}/igv-js-features", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_igv_js_features(
    db: DatabaseDependency, genome_id: str, q: str | None = None
) -> list[m.GenomeFeatureIGV]:
    await get_genome_or_raise_404(db, genome_id)

    results, _ = await db.query_genome_features(
        genome_id, name=q, name_fzy=True, feature_types=["mRNA", "gene", "transcript", "exon"], limit=1
    )

    return [
        m.GenomeFeatureIGV(chromosome=r.contig_name, start=r.entries[0].start_pos, end=r.entries[-1].end_pos)
        for r in results
        if len(r.entries) > 0
    ]


@genome_router.get("/{genome_id}/features.gff3.gz", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_features_gff3(
    config: ConfigDependency,
    db: DatabaseDependency,
    drs_resolver: DrsResolverDependency,
    logger: LoggerDependency,
    request: Request,
    genome_id: str,
):
    # need internal GFF3.gz URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id=genome_id, external_resource_uris=False)

    if not genome.gff3_gz:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Genome with ID {genome_id} has no GFF3 annotation file"
        )

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
        drs_resolver,
        logger,
        genome.gff3_gz,
        range_header,
        "application/gzip",
        impose_response_limit=False,
        support_byte_ranges=True,
    )


@genome_router.get("/{genome_id}/features.gff3.gz.tbi", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_gene_features_gff3_index(
    config: ConfigDependency,
    db: DatabaseDependency,
    drs_resolver: DrsResolverDependency,
    logger: LoggerDependency,
    request: Request,
    genome_id: str,
):
    # need internal GFF3.gz URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id=genome_id, external_resource_uris=False)

    if not genome.gff3_gz_tbi:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Genome with ID {genome_id} has no GFF3 annotation TABIX index",
        )

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
        drs_resolver,
        logger,
        genome.gff3_gz_tbi,
        range_header,
        "application/octet-stream",
        impose_response_limit=False,
        support_byte_ranges=True,
    )
