from typing import AsyncIterator

import aiofiles
import aiohttp
import pathlib

from bento_lib.drs.utils import decode_drs_uri
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from urllib.parse import urlparse

from bento_reference_service.config import Config
from bento_reference_service.constants import RANGE_HEADER_PATTERN

__all__ = [
    "stream_from_uri",
    "generate_uri_streaming_response",
]


async def stream_file(config: Config, path: pathlib.Path, start: int, end: int | None):
    chunk_size = config.file_response_chunk_size

    # TODO: Use range support from FastAPI when it is merged
    async with aiofiles.open(path, "rb") as ff:
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


async def stream_http(config: Config, url: str, headers: dict[str, str]) -> AsyncIterator[bytes]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as res:
            async for chunk in res.content.iter_chunked(config.file_response_chunk_size):
                yield chunk


def exc_bad_range(range_header: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid range header value: {range_header}")


async def drs_bytes_url_from_uri(drs_uri: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(decode_drs_uri(drs_uri)) as res:
            drs_obj = await res.json()
            # TODO: this doesn't support access IDs / the full DRS spec
            https_access = next(filter(lambda am: am["type"] == "https", drs_obj["access_methods"]), None)
            if https_access is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="DRS record for genome does not have an HTTPS access method",
                )
            return https_access["access_url"]["url"]


async def stream_from_uri(config: Config, original_uri: str, range_header: str | None) -> AsyncIterator[bytes]:
    if range_header is None:
        # TODO: send the file if no range header and the FASTA is below some response size limit
        raise NotImplementedError()

    range_header_match = RANGE_HEADER_PATTERN.match(range_header)
    if not range_header_match:
        raise exc_bad_range(range_header)

    start: int
    end: int | None

    try:
        start = int(range_header_match.group(1))
        end_val = range_header_match.group(2)
        end = end_val if end_val is None else int(end_val)
    except ValueError:
        raise exc_bad_range(range_header)

    # TODO: handle parsing exception
    parsed_uri = urlparse(original_uri)
    match parsed_uri.scheme:
        case "file":
            return stream_file(config, pathlib.Path(parsed_uri.path), start, end)

        case "drs" | "http" | "https":
            # Proxy request to HTTP(S) URL, but override media type

            # If this is a DRS URI, we need to first fetch the DRS object record + parse out the access method
            url = drs_bytes_url_from_uri(original_uri) if parsed_uri.scheme == "drs" else original_uri

            # Don't pass Authorization header to possibly external sources
            return stream_http(config, url, headers={"Range": range_header} if range_header else {})

        case _:
            raise ValueError(f"Unsupported URI scheme in genome record: {parsed_uri.scheme}")


async def generate_uri_streaming_response(config: Config, uri: str, range_header: str | None, media_type: str):
    try:
        return StreamingResponse(
            await stream_from_uri(config, uri, range_header),
            media_type=media_type,
            status_code=status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK,
        )
    except ValueError as e:  # Unsupported URI scheme
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
