import io

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel

from .. import models
from ..authz import authz_middleware
from ..config import ConfigDependency
from ..constants import RANGE_HEADER_PATTERN
from ..db import DatabaseDependency
from ..logger import LoggerDependency
from ..models import Alias
from ..streaming import stream_from_uri, generate_uri_streaming_response


__all__ = [
    "refget_router",
]


REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"
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

    # TODO: translate contig fetch into FASTA fetch using FAI data
    #  since FASTAs can have newlines, we need to account for the difference between bytes requested + the bases we
    #  return

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

    # TODO: correct range: accounting for offsets in file from FAI
    return generate_uri_streaming_response(
        config,
        logger,
        genome.fasta,
        "TODO",
        "text/x-fasta",
        impose_response_limit=True,
        extra_response_headers=headers,
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


# TODO: redo for refget 2 properly
@refget_router.get("/service-info", dependencies=[authz_middleware.dep_public_endpoint()])
async def refget_service_info(config: ConfigDependency, response: Response) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET
    # TODO: respond will full service info
    return {
        "refget": {
            "circular_supported": False,
            "algorithms": ["md5", "ga4gh"],
            # I don't like that they used the word 'subsequence' here... that's not what that means exactly.
            # It's a substring!
            "subsequence_limit": config.response_substring_limit,
        }
    }
