import io
import math
import re

from bento_lib.service_info.helpers import build_service_type, build_service_info_from_pydantic_config
from bento_lib.service_info.types import GA4GHServiceInfo
from fastapi import APIRouter, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .. import models, streaming as s, __version__
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..db import DatabaseDependency
from ..fai import parse_fai
from ..logger import LoggerDependency
from ..models import Alias


__all__ = [
    "refget_router",
]

REFGET_VERSION = "2.0.0"
REFGET_SERVICE_TYPE = build_service_type("org.ga4gh", "refget", REFGET_VERSION)

REFGET_HEADER_TEXT = f"text/vnd.ga4gh.refget.v{REFGET_VERSION}+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = f"application/vnd.ga4gh.refget.v{REFGET_VERSION}+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"

RANGE_HEADER_PATTERN = re.compile(r"^bytes=(\d+)-(\d+)?$")

refget_router = APIRouter(prefix="/sequence")


@refget_router.get("/service-info", dependencies=[authz_middleware.dep_public_endpoint()])
async def refget_service_info(
    config: ConfigDependency, logger: LoggerDependency, request: Request, response: Response
) -> dict:
    accept_header: str | None = request.headers.get("Accept", None)
    if accept_header and accept_header not in (
        REFGET_HEADER_JSON_WITH_CHARSET,
        REFGET_HEADER_JSON,
        "application/json",
        "application/*",
        "*/*",
    ):
        raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail="Not Acceptable")

    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET

    genome_service_info: GA4GHServiceInfo = await build_service_info_from_pydantic_config(
        config, logger, {}, REFGET_SERVICE_TYPE, __version__
    )

    del genome_service_info["bento"]

    return {
        **genome_service_info,
        "refget": {
            "circular_supported": False,
            # I don't like that they used the word 'subsequence' here... that's not what that means exactly.
            # It's a substring!
            "subsequence_limit": config.response_substring_limit,
            "algorithms": ["md5", "ga4gh"],
            "identifier_types": [],
        },
    }


REFGET_BAD_REQUEST = Response(status_code=status.HTTP_400_BAD_REQUEST, content=b"Bad Request")
REFGET_RANGE_NOT_SATISFIABLE = Response(
    status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, content=b"Range Not Satisfiable"
)


@refget_router.get("/{sequence_checksum}", dependencies=[authz_middleware.dep_public_endpoint()])
async def refget_sequence(
    config: ConfigDependency,
    logger: LoggerDependency,
    db: DatabaseDependency,
    request: Request,
    sequence_checksum: str,
    start: int | None = None,
    end: int | None = None,
):
    headers = {"Content-Type": REFGET_HEADER_TEXT_WITH_CHARSET, "Accept-Ranges": "bytes"}

    accept_header: str | None = request.headers.get("Accept", None)
    if accept_header and accept_header not in (
        REFGET_HEADER_TEXT_WITH_CHARSET,
        REFGET_HEADER_TEXT,
        "text/plain",
        "text/*",
        "*/*",
    ):
        logger.error(f"not acceptable: bad Accept header value")
        return Response(status_code=status.HTTP_406_NOT_ACCEPTABLE, content=b"Not Acceptable")

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)

    if (start or end) and range_header:
        return REFGET_BAD_REQUEST

    res = await db.get_genome_and_contig_by_checksum_str(sequence_checksum)

    if res is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND, content=b"Not Found")

    genome: models.GenomeWithURIs = res[0]
    contig: models.ContigWithRefgetURI = res[1]

    # Fetch FAI so we can index into FASTA, properly translating the range header for the contig along the way.
    with io.BytesIO() as fb:
        _, _, stream = await s.stream_from_uri(config, logger, genome.fai, None, impose_response_limit=False)
        async for chunk in stream:
            fb.write(chunk)
        fb.seek(0)
        fai_data = fb.read()

    parsed_fai_data = parse_fai(fai_data)
    contig_fai = parsed_fai_data[contig.name]  # TODO: handle lookup error

    start_final: int = 0  # 0-based, inclusive
    end_final: int = contig.length  # 0-based, exclusive

    if start is not None:
        start_final = start
        headers["Accept-Ranges"] = "none"

    if end is not None:
        end_final = end
        headers["Accept-Ranges"] = "none"

    if range_header is not None:
        try:
            intervals = s.parse_range_header(range_header, contig.length, refget_mode=True)
        except s.StreamingBadRange as e:
            logger.error(f"bad request: bad range - {e}")
            return REFGET_BAD_REQUEST
        except s.StreamingRangeNotSatisfiable as e:
            logger.error(f"range not satisfiable: {e}")
            return REFGET_RANGE_NOT_SATISFIABLE

        start_final = intervals[0][0]
        end_final = intervals[0][1] + 1  # range header is inclusive, so we have to adjust it to be exclusive

    if start_final > end_final:
        if not contig.circular:
            logger.error("range not satisfiable: start > end")
            return REFGET_RANGE_NOT_SATISFIABLE
        else:
            raise NotImplementedError()  # TODO: support circular contig querying

    # Final bounds-checking - needed for if we're using query parameters
    if start_final >= contig.length:
        # start is 0-based; so if it's set to contig.length or more, it is out of range.
        logger.error("bad request: start cannot be past the end of the sequence")
        return REFGET_BAD_REQUEST
    if end_final > contig.length:
        # end is 0-based exclusive
        logger.error("bad request: end cannot be past the end of the sequence")
        return REFGET_BAD_REQUEST

    if end_final - start_final > config.response_substring_limit:
        logger.error("range not satisfiable: request for too many bytes")
        return REFGET_RANGE_NOT_SATISFIABLE

    end_final_inclusive: int = end_final - 1  # 0-based, inclusive-indexed

    # Set content length and range based on final start/end values
    headers["Content-Length"] = str(end_final - start_final)
    headers["Content-Range"] = f"bytes {start_final}-{end_final_inclusive}/{contig.length}"

    # Translate contig fetch into FASTA fetch using FAI data:
    #  - since FASTAs can have newlines, we need to account for the difference between bytes requested + the bases we
    #    return

    fai_n_bases, fai_byte_offset, fai_bases_per_line, fai_bytes_per_line_with_newlines = contig_fai

    newline_bytes_per_line = fai_bytes_per_line_with_newlines - fai_bases_per_line
    n_newline_bytes_before_start = int(math.floor(start_final / fai_bases_per_line)) * newline_bytes_per_line
    n_newline_bytes_before_end = int(math.floor(end_final_inclusive / fai_bases_per_line)) * newline_bytes_per_line

    fasta_start_byte = fai_byte_offset + start_final + n_newline_bytes_before_start
    fasta_end_byte = fai_byte_offset + end_final_inclusive + n_newline_bytes_before_end

    fasta_range_header = f"bytes={fasta_start_byte}-{fasta_end_byte}"

    _, _, fasta_stream = await s.stream_from_uri(
        config, logger, genome.fasta, fasta_range_header, impose_response_limit=True
    )

    async def _format_response():
        async for fasta_chunk in fasta_stream:
            yield fasta_chunk.replace(b"\n", b"").replace(b"\r", b"")

    return StreamingResponse(
        _format_response(),
        headers=headers,
        media_type="text/x-fasta",
        status_code=status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK,
    )


class RefGetSequenceMetadata(BaseModel):
    md5: str
    ga4gh: str
    length: int
    aliases: list[Alias]


class RefGetSequenceMetadataResponse(BaseModel):
    metadata: RefGetSequenceMetadata


@refget_router.get("/{sequence_checksum}/metadata", dependencies=[authz_middleware.dep_public_endpoint()])
async def refget_sequence_metadata(
    db: DatabaseDependency,
    response: Response,
    sequence_checksum: str,
) -> RefGetSequenceMetadataResponse:
    res: tuple[str, models.ContigWithRefgetURI] | None = await db.get_genome_and_contig_by_checksum_str(
        sequence_checksum
    )

    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET

    if res is None:
        # TODO: proper 404 for refget spec
        # TODO: proper content type for exception - RefGet error class?
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"sequence not found with checksum: {sequence_checksum}",
        )

    contig = res[1]
    return RefGetSequenceMetadataResponse(
        metadata=RefGetSequenceMetadata(
            md5=contig.md5,
            ga4gh=contig.ga4gh,
            length=contig.length,
            aliases=contig.aliases,
        ),
    )
