import aiofiles
import aiofiles.os
import aiohttp
import logging
import pathlib

from bento_lib.drs.utils import decode_drs_uri
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from typing import AsyncIterator
from urllib.parse import urlparse

from bento_reference_service.config import Config
from bento_reference_service.constants import RANGE_HEADER_PATTERN

__all__ = [
    "StreamingRangeNotSatisfiable",
    "stream_from_uri",
    "generate_uri_streaming_response",
]


class StreamingRangeNotSatisfiable(Exception):
    def __init__(self, message: str, n_bytes: int | None):
        self._n_bytes: int | None = n_bytes
        super().__init__(message)

    @property
    def n_bytes(self) -> int:
        return self._n_bytes


class StreamingBadRange(Exception):
    pass


class StreamingProxyingError(Exception):
    pass


class StreamingResponseExceededLimit(Exception):
    pass


class StreamingBadURI(Exception):
    pass


class StreamingUnsupportedURIScheme(Exception):
    pass


def tcp_connector(config: Config) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(verify_ssl=config.bento_validate_ssl)


async def stream_file(
    config: Config,
    path: pathlib.Path,
    start: int,
    end: int | None,
    yield_content_length_as_first_8: bool = False,
):
    file_size = (await aiofiles.os.stat(path)).st_size
    chunk_size = config.file_response_chunk_size

    if start >= file_size or (end is not None and end >= file_size):
        # both ends of the range are 0-indexed, inclusive - so it starts at 0 and ends at file_size - 1
        raise StreamingRangeNotSatisfiable(f"Range not satisfiable while streaming {path}", file_size)

    if end is not None and (start > end):
        raise StreamingBadRange()

    if yield_content_length_as_first_8:
        yield file_size.to_bytes(8, "big")

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


async def stream_http(
    config: Config,
    url: str,
    headers: dict[str, str],
    yield_content_length_as_first_8: bool = False,
) -> AsyncIterator[bytes]:
    async with aiohttp.ClientSession(connector=tcp_connector(config)) as session:
        async with session.get(url, headers=headers) as res:
            if res.status == status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE:
                n_bytes = None
                if (crh := res.headers.get("Content-Range")) is not None and crh.startswith("bytes */"):
                    n_bytes = int(crh.split("/")[-1])
                raise StreamingRangeNotSatisfiable(f"Range not satisfiable while streaming {url}", n_bytes)

            elif res.status > 299:
                err_content = (await res.content.read()).decode("utf-8")
                raise StreamingProxyingError(f"Error while streaming {url}: {res.status} {err_content}")

            if yield_content_length_as_first_8:
                yield int(res.headers["Content-Length"]).to_bytes(8, "big")
            async for chunk in res.content.iter_chunked(config.file_response_chunk_size):
                yield chunk


async def drs_bytes_url_from_uri(config: Config, drs_uri: str) -> str:
    async with aiohttp.ClientSession(connector=tcp_connector(config)) as session:
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


async def stream_from_uri(
    config: Config, original_uri: str, range_header: str | None, impose_response_limit: bool
) -> tuple[int, AsyncIterator[bytes]]:
    stream: AsyncIterator[bytes]

    try:
        parsed_uri = urlparse(original_uri)
    except ValueError:
        raise StreamingBadURI(f"Bad URI: {original_uri}")

    match parsed_uri.scheme:
        case "file":
            start: int = 0
            end: int | None = None

            if range_header:
                range_header_match = RANGE_HEADER_PATTERN.match(range_header)
                if not range_header_match:
                    raise StreamingBadRange()

                try:
                    start = int(range_header_match.group(1))
                    end_val = range_header_match.group(2)
                    end = end_val if end_val is None else int(end_val)
                except ValueError:
                    raise StreamingBadRange()

            stream = stream_file(
                config, pathlib.Path(parsed_uri.path), start, end, yield_content_length_as_first_8=True
            )

        case "drs" | "http" | "https":
            # Proxy request to HTTP(S) URL, but override media type

            # If this is a DRS URI, we need to first fetch the DRS object record + parse out the access method
            url = await drs_bytes_url_from_uri(config, original_uri) if parsed_uri.scheme == "drs" else original_uri

            # Don't pass Authorization header to possibly external sources
            stream = stream_http(
                config,
                url,
                headers={"Range": range_header} if range_header else {},
                yield_content_length_as_first_8=True,
            )

        case _:
            raise StreamingUnsupportedURIScheme(parsed_uri.scheme)

    # Content length should be the first 8 bytes of the stream
    content_length = int.from_bytes(await anext(stream), "big")

    if impose_response_limit and content_length > config.response_substring_limit:
        raise StreamingResponseExceededLimit()

    async def _agen():
        async for chunk in stream:
            yield chunk

    return content_length, _agen()


async def generate_uri_streaming_response(
    config: Config,
    logger: logging.Logger,
    uri: str,
    range_header: str | None,
    media_type: str,
    impose_response_limit: bool,
    extra_response_headers: dict[str, str] | None = None,
):
    try:
        content_length, stream = await stream_from_uri(config, uri, range_header, impose_response_limit)
        return StreamingResponse(
            stream,
            headers={**(extra_response_headers or {}), "Content-Length": str(content_length)},
            media_type=media_type,
            status_code=status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK,
        )
    except StreamingRangeNotSatisfiable as e:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers={"Content-Range": f"bytes */{e.n_bytes}"},
        )
    except StreamingBadRange:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid range header value: {range_header}"
        )
    except StreamingProxyingError as e:  #
        logger.error(f"Encountered streaming error for {uri}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except StreamingUnsupportedURIScheme as e:  # Unsupported URI scheme
        err = f"Unsupported URI scheme in genome record: {e}"
        logger.error(err)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)
    except StreamingBadURI as e:  # URI parsing error
        err = f"Bad URI in genome record: {e}"
        logger.error(err)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)
