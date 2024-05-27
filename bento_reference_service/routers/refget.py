import io
import math

from bento_lib.service_info.helpers import build_service_type, build_service_info_from_pydantic_config
from bento_lib.service_info.types import GA4GHServiceInfo
from fastapi import APIRouter, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .. import models, __version__
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..constants import RANGE_HEADER_PATTERN
from ..db import DatabaseDependency
from ..logger import LoggerDependency
from ..models import Alias
from ..streaming import stream_from_uri


__all__ = [
    "refget_router",
]

REFGET_VERSION = "2.0.0"
REFGET_HEADER_TEXT = f"text/vnd.ga4gh.refget.v{REFGET_VERSION}+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = f"application/vnd.ga4gh.refget.v{REFGET_VERSION}+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"

refget_router = APIRouter(prefix="/sequence")


def parse_fai(fai_data: bytes) -> dict[str, tuple[int, int, int, int]]:
    res: dict[str, tuple[int, int, int, int]] = {}

    for record in fai_data.split(b"\n"):
        if not record:  # trailing newline or whatever
            continue

        row = record.split(b"\t")
        if len(row) != 5:
            raise ValueError(f"Invalid FAI record: {record.decode('ascii')}")

        # FAI record: contig, (num bases, byte index, bases per line, bytes per line)
        res[row[0].decode("ascii")] = (int(row[1]), int(row[2]), int(row[3]), int(row[4]))

    return res


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
        config, logger, {}, build_service_type("org.ga4gh", "refget", REFGET_VERSION), __version__
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
        # TODO: plain text error:
        raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail="Not Acceptable")

    # Don't use FastAPI's auto-Header tool for the Range header
    # 'cause I don't want to shadow Python's range() function
    range_header: str | None = request.headers.get("Range", None)

    if (start or end) and range_header:
        # TODO: Valid plain text error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="cannot specify both start/end and Range header"
        )

    res = await db.get_genome_and_contig_by_checksum_str(sequence_checksum)

    if res is None:
        # TODO: proper 404 for refget spec
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"sequence not found with checksum: {sequence_checksum}",
        )

    genome: models.GenomeWithURIs = res[0]
    contig: models.ContigWithRefgetURI = res[1]

    # Fetch FAI so we can index into FASTA, properly translating the range header for the contig along the way.
    with io.BytesIO() as fb:
        _, stream = await stream_from_uri(config, logger, genome.fai, None, impose_response_limit=False)
        async for chunk in stream:
            fb.write(chunk)
        fb.seek(0)
        fai_data = fb.read()

    parsed_fai_data = parse_fai(fai_data)
    contig_fai = parsed_fai_data[contig.name]  # TODO: handle lookup error

    # TODO: correct refget-formatted errors

    start_final: int = 0  # 0-based, inclusive
    end_final: int = contig.length - 1  # 0-based, exclusive - need to adjust range (which is inclusive)

    if start is not None:
        if end is not None:
            headers["Accept-Ranges"] = "none"
            if start > end:
                if not contig.circular:
                    raise HTTPException(
                        status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Range Not Satisfiable"
                    )
                else:
                    raise NotImplementedError()  # TODO: support circular contig querying
            end_final = end
        start_final = start

    if range_header is not None:
        range_header_match = RANGE_HEADER_PATTERN.match(range_header)
        if not range_header_match:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bad range")

        try:
            start_final = int(range_header_match.group(1))
            if end_val := range_header_match.group(2):
                end_final = end_val + 1  # range is inclusive, so we have to adjust it to be exclusive
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bad range")

    # Final bounds-checking
    if start_final >= contig.length:
        # start is 0-based; so if it's set to contig.length or more, it is out of range.
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start cannot be longer than sequence")
    if end_final > contig.length:
        # end is 0-based inclusive
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="end cannot be past the end of the sequence"
        )

    if end_final - start_final > config.response_substring_limit:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="request for too many bytes"
        )  # TODO: what is real error?

    # Translate contig fetch into FASTA fetch using FAI data:
    #  - since FASTAs can have newlines, we need to account for the difference between bytes requested + the bases we
    #    return

    fai_n_bases, fai_byte_offset, fai_bases_per_line, fai_bytes_per_line_with_newlines = contig_fai

    newline_bytes_per_line = fai_bytes_per_line_with_newlines - fai_bases_per_line
    n_newline_bytes_before_start = int(math.floor(start_final / fai_bases_per_line)) * newline_bytes_per_line
    n_newline_bytes_before_end = int(math.floor(end_final / fai_bases_per_line)) * newline_bytes_per_line

    fasta_start_byte = fai_byte_offset + start_final + n_newline_bytes_before_start
    fasta_end_byte = fai_byte_offset + end_final + n_newline_bytes_before_end

    fasta_range_header = f"Range: bytes={fasta_start_byte}-{fasta_end_byte}"

    _, fasta_stream = await stream_from_uri(
        config,
        logger,
        genome.fasta,
        fasta_range_header,
        impose_response_limit=True,
    )

    async def _format_response():
        async for fasta_chunk in fasta_stream:
            yield fasta_chunk.replace(b"\n", b"").replace(b"\r", b"")

    stream = _format_response()

    return StreamingResponse(
        stream,
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
