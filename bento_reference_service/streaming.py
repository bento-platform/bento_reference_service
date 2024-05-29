import aiofiles
import aiofiles.os
import aiohttp
import json
import logging
import pathlib
import re

from bento_lib.drs.utils import decode_drs_uri
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from typing import AsyncIterator
from urllib.parse import urlparse

from bento_reference_service.config import Config

__all__ = [
    "parse_range_header",
    "StreamingRangeNotSatisfiable",
    "stream_from_uri",
    "generate_uri_streaming_response",
]


ACCEPT_BYTE_RANGES = {"Accept-Ranges": "bytes"}

BYTE_RANGE_INTERVAL_SPLIT = re.compile(r",\s*")
BYTE_RANGE_START_ONLY = re.compile(r"^(\d+)-$")
BYTE_RANGE_START_END = re.compile(r"^(\d+)-(\d+)$")
BYTE_RANGE_SUFFIX = re.compile(r"^-(\d+)$")


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
    return aiohttp.TCPConnector(ssl=config.bento_validate_ssl)


def parse_range_header(
    range_header: str | None, content_length: int, refget_mode: bool = False
) -> tuple[tuple[int, int], ...]:
    """
    Parse a range header (given a particular content length) into a validated series of sorted, non-overlapping
    start/end-inclusive intervals.
    """

    if range_header is None:
        return ((0, content_length),)

    intervals: list[tuple[int, int]] = []

    if not range_header.startswith("bytes="):
        raise StreamingBadRange("only bytes range headers are supported")

    intervals_str = range_header.removeprefix("bytes=")

    # Cases: start- | start-end | -suffix, [start- | start-end | -suffix], ...

    intervals_str_split = BYTE_RANGE_INTERVAL_SPLIT.split(intervals_str)

    for iv in intervals_str_split:
        if m := BYTE_RANGE_START_ONLY.match(iv):
            intervals.append((int(m.group(1)), content_length - 1))
        elif m := BYTE_RANGE_START_END.match(iv):
            intervals.append((int(m.group(1)), int(m.group(2))))
        elif m := BYTE_RANGE_SUFFIX.match(iv):
            inclusive_content_length = content_length - 1
            suffix_length = int(m.group(1))  # suffix: -500 === last 500:
            intervals.append((max(inclusive_content_length - suffix_length + 1, 0), inclusive_content_length))
        else:
            raise StreamingBadRange("byte range did not match any pattern")

    intervals.sort()
    n_intervals: int = len(intervals)

    # validate intervals are not inverted and do not overlap each other:
    for i, int1 in enumerate(intervals):
        int1_start, int1_end = int1

        # Order of these checks is important - we want to give a 416 if start/end is beyond content length (which also
        # results in an inverted interval)

        if int1_start >= content_length:
            # both ends of the range are 0-indexed, inclusive - so it starts at 0 and ends at content_length - 1
            if refget_mode:  # sigh... GA4GH moment
                raise StreamingBadRange(f"start is beyond content length: {int1_start} >= {content_length}")
            raise StreamingRangeNotSatisfiable(
                f"not satisfiable: {int1_start} >= {content_length}", content_length
            )

        if int1_end >= content_length:
            # both ends of the range are 0-indexed, inclusive - so it starts at 0 and ends at content_length - 1
            if refget_mode:  # sigh... GA4GH moment
                raise StreamingBadRange(f"end is beyond content length: {int1_end} >= {content_length}")
            raise StreamingRangeNotSatisfiable(f"not satisfiable: {int1_end} >= {content_length}", content_length)

        if not refget_mode and int1_start > int1_end:
            raise StreamingRangeNotSatisfiable(f"inverted interval: {int1}", content_length)

        if i < n_intervals - 1:
            int2 = intervals[i + 1]
            int2_start, int2_end = int2

            if int1_end >= int2_start:
                raise StreamingRangeNotSatisfiable(f"intervals overlap: {int1}, {int2}", content_length)

    return tuple(intervals)


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
                raise StreamingRangeNotSatisfiable(f"Range not satisfiable while streaming {url}", n_bytes)

            elif res.status > 299:
                err_content = (await res.content.read()).decode("utf-8")
                raise StreamingProxyingError(f"Error while streaming {url}: {res.status} {err_content}")

            if yield_status_as_first_2:
                yield res.status.to_bytes(2, "big")

            if yield_content_length_as_next_8:
                if "Content-Length" not in res.headers:
                    raise StreamingProxyingError(f"Error while streaming {url}: missing Content-Length header")
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
        raise StreamingBadURI(f"Bad URI: {original_uri}")

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
            raise StreamingUnsupportedURIScheme(parsed_uri.scheme)

    # Content length should be the next 8 bytes of the stream
    content_length = int.from_bytes(await anext(stream), "big")

    if impose_response_limit and content_length > config.response_substring_limit:
        raise StreamingResponseExceededLimit()

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
