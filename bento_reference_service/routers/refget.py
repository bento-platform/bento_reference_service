import pysam

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel

from bento_reference_service import models
from bento_reference_service.config import ConfigDependency
from bento_reference_service.constants import RANGE_HEADER_PATTERN
from bento_reference_service.db import DatabaseDependency
from bento_reference_service.models import Alias


__all__ = [
    "refget_router",
]


REFGET_HEADER_TEXT = "text/vnd.ga4gh.refget.v1.0.1+plain"
REFGET_HEADER_TEXT_WITH_CHARSET = f"{REFGET_HEADER_TEXT}; charset=us-ascii"
REFGET_HEADER_JSON = "application/vnd.ga4gh.refget.v1.0.1+json"
REFGET_HEADER_JSON_WITH_CHARSET = f"{REFGET_HEADER_JSON}; charset=us-ascii"

refget_router = APIRouter(prefix="/sequence")


@refget_router.get("/{sequence_checksum}")
async def refget_sequence(
    config: ConfigDependency,
    db: DatabaseDependency,
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="cannot specify both start/end and Range header"
        )

    res: tuple[str, models.ContigWithRefgetURI] | None = await db.get_genome_id_and_contig_by_checksum_str(
        sequence_checksum
    )

    if res is None:
        # TODO: proper 404 for refget spec
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"sequence not found with checksum: {sequence_checksum}",
        )

    contig: models.ContigWithRefgetURI = res[1]

    # TODO: fetch FAI, translate contig fetch into FASTA fetch

    start_final: int = 0  # 0-based, inclusive
    end_final: int = contig.length - 1  # 0-based, exclusive - need to adjust range (which is inclusive)

    if start is not None:
        if end is not None:
            response.headers["Accept-Ranges"] = "none"
            if start > end:
                if not contig.circular:
                    raise HTTPException(
                        status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Range Not Satisfiable"
                    )
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

    genome = await db.get_genome(res[0])

    fa = pysam.FastaFile(filename=str(genome.fasta), filepath_index=str(genome.fai))
    try:
        # TODO: handle missing region / coordinate exceptions explicitly
        return fa.fetch(contig.name, start_final, end_final).encode("ascii")
    finally:
        fa.close()


class RefGetSequenceMetadata(BaseModel):
    md5: str
    trunc512: str
    length: int
    aliases: list[Alias]


class RefGetSequenceMetadataResponse(BaseModel):
    metadata: RefGetSequenceMetadata


@refget_router.get("/{sequence_checksum}/metadata")
async def refget_sequence_metadata(
    db: DatabaseDependency,
    response: Response,
    sequence_checksum: str,
) -> RefGetSequenceMetadataResponse:
    res: tuple[str, models.ContigWithRefgetURI] | None = await db.get_genome_id_and_contig_by_checksum_str(
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
            trunc512=contig.trunc512,
            length=contig.length,
            aliases=contig.aliases,
        ),
    )


# TODO: redo for refget 2 properly - this endpoint doesn't exist anymore
@refget_router.get("/service-info")
async def refget_service_info(config: ConfigDependency, response: Response) -> dict:
    response.headers["Content-Type"] = REFGET_HEADER_JSON_WITH_CHARSET
    return {
        "service": {
            "circular_supported": False,
            "algorithms": ["md5", "ga4gh"],
            # I don't like that they used the word 'subsequence' here... that's not what that means exactly.
            # It's a substring!
            "subsequence_limit": config.response_substring_limit,
            "supported_api_versions": ["2.0"],
        }
    }
