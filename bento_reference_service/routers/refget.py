import logging

import pysam

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, HTTPException, Request, Response

from bento_reference_service import indices, models
from bento_reference_service.config import Config, ConfigDependency
from bento_reference_service.constants import RANGE_HEADER_PATTERN
from bento_reference_service.es import ESDependency
from bento_reference_service.genomes import get_genomes_with_uris, get_genome_or_error
from bento_reference_service.logger import LoggerDependency


__all__ = [
    "refget_router",
]


REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"

refget_router = APIRouter(prefix="/sequence")


async def get_contig_by_checksum(
    checksum: str,
    # Dependencies
    config: Config,
    es: AsyncElasticsearch,
    logger: logging.Logger,
) -> models.Contig | None:
    genome_index = indices.make_genome_index_def(config)

    es_resp = await es.search(
        index=genome_index["name"],
        query={
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
        },
    )

    if es_resp["hits"]["total"]["value"] > 0:
        # Use ES result, since we got a hit
        sg = es_resp["hits"]["hits"][0]

        # We have the genome, but we still need to extract the contig from inner hits
        if sg["inner_hits"]["contigs"]["hits"]["total"]["value"] > 0:
            # We have a contig hit, so return it
            return models.Contig(**sg["inner_hits"]["contigs"]["hits"]["hits"][0]["_source"])

        for sc in sg["contigs"]:
            if checksum in (sc["md5"], sc["trunc512"]):
                return models.Contig.model_validate(sc)

        logger.error(f"Found ES hit for checksum {checksum} but could not find contig in inner hits")

    logger.debug(f"No hits in ES index for checksum {checksum}")

    # Manually iterate as a fallback
    async for genome in get_genomes_with_uris(config, logger):
        for sc in genome.contigs:
            if checksum in (sc.md5, sc.trunc512):
                logger.warning(f"Found manual hit for {checksum}, but no corresponding entry in ES index")
                return sc

    return None


@refget_router.get("/{sequence_checksum}")
async def refget_sequence(
    config: ConfigDependency,
    es: ESDependency,
    logger: LoggerDependency,
    request: Request,
    response: Response,
    sequence_checksum: str,
    start: int | None = None,
    end: int | None = None,
):
    response.headers["Content-Type"] = REFGET_HEADER_TEXT_WITH_CHARSET

    accept_header: str | None = request.headers.get("Accept", None)
    if accept_header and accept_header not in (
        REFGET_HEADER_TEXT_WITH_CHARSET,
        REFGET_HEADER_TEXT,
        "text/plain",
    ):
        raise HTTPException(status_code=406, detail="Not Acceptable")  # TODO: plain text error

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)

    if (start or end) and range_header:
        # TODO: Valid plain text error
        raise HTTPException(status_code=400, detail="cannot specify both start/end and Range header")

    contig: models.Contig | None = await get_contig_by_checksum(sequence_checksum, config, es, logger)

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
        raise HTTPException(
            status_code=404,
            detail=f"sequence not found with checksum: {sequence_checksum}",
        )

    if end_final - start_final > config.response_substring_limit:
        raise HTTPException(status_code=400, detail="request for too many bytes")  # TODO: what is real error?

    genome = await get_genome_or_error(contig.genome, config)

    fa = pysam.FastaFile(filename=str(genome.fasta), filepath_index=str(genome.fai))
    try:
        # TODO: handle missing region / coordinate exceptions explicitly
        return fa.fetch(contig.name, start_final, end_final).encode("ascii")
    finally:
        fa.close()


@refget_router.get("/{sequence_checksum}/metadata")
async def refget_sequence_metadata(
    config: ConfigDependency,
    es: ESDependency,
    logger: LoggerDependency,
    response: Response,
    sequence_checksum: str,
) -> dict:  # TODO: type: refget resp
    contig: models.Contig | None = await get_contig_by_checksum(sequence_checksum, config, es, logger)

    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET

    if contig is None:
        # TODO: proper 404 for refget spec
        # TODO: proper content type for exception - RefGet error class?
        raise HTTPException(
            status_code=404,
            detail=f"sequence not found with checksum: {sequence_checksum}",
        )

    return {
        "metadata": {
            "md5": contig.md5,
            "trunc512": contig.trunc512,
            "length": contig.length,
            "aliases": [a.model_dump(mode="json") for a in contig.aliases],
        },
    }


@refget_router.get("/service-info")
async def refget_service_info(config: ConfigDependency, response: Response) -> dict:
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
