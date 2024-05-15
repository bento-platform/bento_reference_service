import aiofiles
import asyncpg
import traceback

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import StreamingResponse
from typing import Annotated
from uuid import uuid4

from .. import models as m
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..db import Database, DatabaseDependency
from ..features import INGEST_FEATURES_TASK_KIND, ingest_features_task
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
    db: DatabaseDependency, response_format: str | None = None
) -> tuple[m.GenomeWithURIs, ...] | tuple[str, ...]:
    genomes = await db.get_genomes(external_resource_uris=True)
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
        logger.error(f"UniqueViolationError encountered during genome creation: {e} {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Genome with ID {genome.id} already exists",
        )


# Put FASTA/FAI endpoints ahead of detail endpoint, so they get handled first, and we fall back to treating the whole
# /genomes/<...> as the genome ID.


@genome_router.get("/{genome_id}.fa", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta(
    genome_id: str, config: ConfigDependency, db: DatabaseDependency, logger: LoggerDependency, request: Request
) -> StreamingResponse:
    # need internal FASTA URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id, external_resource_uris=False)

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
        logger,
        genome.fasta,
        range_header,
        "text/x-fasta",
        impose_response_limit=False,
        support_byte_ranges=True,
    )


@genome_router.get("/{genome_id}.fa.fai", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_fasta_index(
    genome_id: str, config: ConfigDependency, db: DatabaseDependency, logger: LoggerDependency, request: Request
) -> StreamingResponse:
    # need internal FAI URI:
    genome: m.Genome = await get_genome_or_raise_404(db, genome_id, external_resource_uris=False)

    # Don't use FastAPI's auto-Header tool for the Range header 'cause I don't want to shadow Python's range() function:
    range_header: str | None = request.headers.get("Range", None)
    return await generate_uri_streaming_response(
        config,
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
    name: str | None = None,
    position: str | None = None,
    start: int | None = None,
    end: int | None = None,
    feature_type: Annotated[list[str] | None, Query()] = None,
    offset: int = 0,
    limit: int = 10,
):
    if q:
        results, pagination = await db.query_genome_features(genome_id, q, offset, limit)
    else:
        results, pagination = await db.filter_genome_features(
            genome_id, name, position, start, end, feature_type, offset, limit
        )

    return {
        "results": results,
        "pagination": pagination,
    }


@genome_router.delete(
    "/{genome_id}/features",
    dependencies=[DEPENDENCY_DELETE_REFERENCE_MATERIAL],
    status_code=status.HTTP_204_NO_CONTENT,
)
async def genomes_detail_features_delete(db: DatabaseDependency, genome_id: str):
    await db.clear_genome_features(genome_id)


@genome_router.get("/{genome_id}/features/{feature_id}", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_features_detail(db: DatabaseDependency, genome_id: str, feature_id: str):
    return await db.get_genome_feature_by_id(genome_id, feature_id)


@genome_router.get("/{genome_id}/features.gff3.gz", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_features_gff3(
    config: ConfigDependency, db: DatabaseDependency, logger: LoggerDependency, request: Request, genome_id: str
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
        logger,
        genome.gff3_gz,
        range_header,
        "application/gzip",
        impose_response_limit=False,
        support_byte_ranges=True,
    )


@genome_router.put(
    "/{genome_id}/features.gff3.gz",
    dependencies=[DEPENDENCY_INGEST_REFERENCE_MATERIAL],
    status_code=status.HTTP_202_ACCEPTED,
)
async def genomes_detail_features_ingest_gff3(
    background_tasks: BackgroundTasks,
    config: ConfigDependency,
    db: DatabaseDependency,
    logger: LoggerDependency,
    genome_id: str,
    gff3_gz: UploadFile,
    gff3_gz_tbi: UploadFile,
):
    # Verify that genome exists
    await get_genome_or_raise_404(db, genome_id=genome_id, external_resource_uris=False)

    fn = config.file_ingest_tmp_dir / f"{uuid4()}.gff3.gz"
    fn_tbi = config.file_ingest_tmp_dir / f"{fn}.tbi"

    # copy .gff3.gz to temporary directory for ingestion
    async with aiofiles.open(fn, "wb") as fh:
        while data := (await gff3_gz.read(config.file_response_chunk_size)):
            await fh.write(data)

    logger.debug(f"Wrote GFF.gz data to {fn}; size={fn.stat().st_size}")

    # copy .gff3.gz.tbi to temporary directory for ingestion
    async with aiofiles.open(fn_tbi, "wb") as fh:
        while data := (await gff3_gz_tbi.read(config.file_response_chunk_size)):
            await fh.write(data)

    logger.debug(f"Wrote GFF.gz.tbi data to {fn_tbi}; size={fn_tbi.stat().st_size}")

    task_id = await db.create_task(genome_id, INGEST_FEATURES_TASK_KIND)
    background_tasks.add_task(ingest_features_task, genome_id, fn, fn_tbi, task_id, db, logger)
    return {"task": f"{config.service_url_base_path}/tasks/{task_id}"}


@genome_router.get("/{genome_id}/features.gff3.gz.tbi", dependencies=[authz_middleware.dep_public_endpoint()])
async def genomes_detail_gene_features_gff3_index(
    config: ConfigDependency, db: DatabaseDependency, logger: LoggerDependency, request: Request, genome_id: str
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
        logger,
        genome.gff3_gz_tbi,
        range_header,
        "application/octet-stream",
        impose_response_limit=False,
        support_byte_ranges=True,
    )
