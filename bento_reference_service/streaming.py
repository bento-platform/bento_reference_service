import aiofiles
import aiofiles.os
import aiohttp
import json
import logging
import pathlib

from bento_lib.drs.utils import decode_drs_uri
from bento_lib.streaming import exceptions as se
from bento_lib.streaming.range import parse_range_header
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from typing import AsyncIterator
from urllib.parse import urlparse

from bento_reference_service.config import Config

__all__ = [
    "stream_from_uri",
    "generate_uri_streaming_response",
]


ACCEPT_BYTE_RANGES = {"Accept-Ranges": "bytes"}


def tcp_connector(config: Config) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(ssl=config.bento_validate_ssl)


async def stream_file(
    config: Config,
    path: pathlib.Path,
    range_header: str | None,
    yield_content_length_as_first_8: bool = False,
):
    """
    Stream the contents of a file, optionally yielding the content length as the first 8 bytes of the stream.
    Coordinate parameters are 0-based and inclusive, e.g., 0-10 yields the first 11 bytes. This matches the format of
    HTTP range headers.
    """

    file_size = (await aiofiles.os.stat(path)).st_size
    intervals = parse_range_header(range_header, file_size)

    # for now, only support returning a single range of bytes; take the start and end from the first interval given:
    start, end = intervals[0]
    response_size: int = end - start + 1

    # TODO: support multipart/byterange responses

    if yield_content_length_as_first_8:
        yield response_size.to_bytes(8, "big")

    chunk_size = config.file_response_chunk_size

    async with aiofiles.open(path, "rb") as ff:
        # Logic mostly ported from bento_drs

        # First, skip over <start> bytes to get to the beginning of the range
        await ff.seek(start)

        byte_offset: int = start
        while True:
            # Add a 1 to the amount to read if it's below chunk size, because the last coordinate is inclusive.
            data = await ff.read(min(chunk_size, end + 1 - byte_offset))
            byte_offset += len(data)
            yield data

            # If we've hit the end of the file and are reading empty byte strings, or we've reached the
            # end of our range (inclusive), then escape the loop.
            # This is guaranteed to terminate with a finite-sized file.
            if not data or byte_offset > end:
                break


async def stream_http(
    config: Config,
    url: str,
    headers: dict[str, str],
    yield_status_as_first_2: bool = False,
    yield_content_length_as_next_8: bool = False,
) -> AsyncIterator[bytes]:
    async with aiohttp.ClientSession(connector=tcp_connector(config)) as session:
        async with session.get(url, headers=headers) as res:
            if res.status == status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE:
                n_bytes = None
                if (crh := res.headers.get("Content-Range")) is not None and crh.startswith("bytes */"):
                    n_bytes = int(crh.split("/")[-1])
                raise se.StreamingRangeNotSatisfiable(f"Range not satisfiable while streaming {url}", n_bytes)

            elif res.status > 299:
                err_content = (await res.content.read()).decode("utf-8")
                raise se.StreamingProxyingError(f"Error while streaming {url}: {res.status} {err_content}")

            if yield_status_as_first_2:
                yield res.status.to_bytes(2, "big")

            if yield_content_length_as_next_8:
                if "Content-Length" not in res.headers:
                    raise se.StreamingProxyingError(f"Error while streaming {url}: missing Content-Length header")
                yield int(res.headers["Content-Length"]).to_bytes(8, "big")

            async for chunk in res.content.iter_chunked(config.file_response_chunk_size):
                yield chunk


async def drs_bytes_url_from_uri(config: Config, logger: logging.Logger, drs_uri: str) -> str:
    async with aiohttp.ClientSession(connector=tcp_connector(config)) as session:
        async with session.get(decoded_uri := decode_drs_uri(drs_uri)) as res:
            if res.status != status.HTTP_200_OK:
                logger.error(
                    f"Error encountered while accessing DRS record: {drs_uri} (decoded to {decoded_uri}); got "
                    f"{res.status} {(await res.content.read()).decode('utf-8')}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"A {res.status} error was encountered while accessing DRS record for genome",
                )

            drs_obj = await res.json()
            # TODO: this doesn't support access IDs / the full DRS spec
            logger.debug(f"{drs_uri} (decoded to {decoded_uri}): got DRS response {json.dumps(drs_obj)}")
            https_access = next(filter(lambda am: am["type"] == "https", drs_obj.get("access_methods", [])), None)
            if https_access is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="DRS record for genome does not have an HTTPS access method",
                )
            return https_access["access_url"]["url"]


async def stream_from_uri(
    config: Config, logger: logging.Logger, original_uri: str, range_header: str | None, impose_response_limit: bool
) -> tuple[int, int, AsyncIterator[bytes]]:
    stream: AsyncIterator[bytes]

    try:
        parsed_uri = urlparse(original_uri)
    except ValueError:
        raise se.StreamingBadURI(f"Bad URI: {original_uri}")

    match parsed_uri.scheme:
        case "file":
            stream = stream_file(
                config, pathlib.Path(parsed_uri.path), range_header, yield_content_length_as_first_8=True
            )
            status_code = status.HTTP_206_PARTIAL_CONTENT if range_header else status.HTTP_200_OK

        case "drs" | "http" | "https":
            # Proxy request to HTTP(S) URL, but override media type

            # If this is a DRS URI, we need to first fetch the DRS object record + parse out the access method
            url = (
                await drs_bytes_url_from_uri(config, logger, original_uri)
                if parsed_uri.scheme == "drs"
                else original_uri
            )

            # Don't pass Authorization header to possibly external sources
            logger.debug(f"Streaming from HTTP URL: {url}")
            stream = stream_http(
                config,
                url,
                headers={"Range": range_header} if range_header else {},
                yield_status_as_first_2=True,
                yield_content_length_as_next_8=True,
            )
            status_code = int.from_bytes(await anext(stream), "big")  # 2 bytes specifying status code

        case _:
            raise se.StreamingUnsupportedURIScheme(parsed_uri.scheme)

    # Content length should be the next 8 bytes of the stream
    content_length = int.from_bytes(await anext(stream), "big")

    if impose_response_limit and content_length > config.response_substring_limit:
        raise se.StreamingResponseExceededLimit()

    async def _agen():
        async for chunk in stream:
            yield chunk

    return content_length, status_code, _agen()


async def generate_uri_streaming_response(
    config: Config,
    logger: logging.Logger,
    uri: str,
    range_header: str | None,
    media_type: str,
    impose_response_limit: bool,
    support_byte_ranges: bool = False,
    extra_response_headers: dict[str, str] | None = None,
):
    try:
        content_length, status_code, stream = await stream_from_uri(
            config, logger, uri, range_header, impose_response_limit
        )
        return StreamingResponse(
            stream,
            headers={
                **(extra_response_headers or {}),
                **(ACCEPT_BYTE_RANGES if support_byte_ranges else {}),
                "Content-Length": str(content_length),
            },
            media_type=media_type,
            status_code=status_code,
        )
    except se.StreamingRangeNotSatisfiable as e:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers={"Content-Range": f"bytes */{e.n_bytes}"},
        )
    except se.StreamingBadRange:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid range header value: {range_header}"
        )
    except se.StreamingProxyingError as e:  #
        logger.error(f"Encountered streaming error for {uri}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except se.StreamingUnsupportedURIScheme as e:  # Unsupported URI scheme
        err = f"Unsupported URI scheme in genome record: {e}"
        logger.error(err)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)
    except se.StreamingBadURI as e:  # URI parsing error
        err = f"Bad URI in genome record: {e}"
        logger.error(err)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)
