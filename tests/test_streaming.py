import logging
import pytest

from aioresponses import aioresponses
from fastapi import HTTPException, status
from typing import Type

from bento_reference_service import config as c, streaming as s

from .shared_data import SARS_COV_2_FASTA_PATH

HTTP_TEST_URI = "https://test.local/file.txt"

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "range_header,exc",
    [
        ("bites=0-100", s.StreamingBadRange),
        ("bytes=500", s.StreamingBadRange),
        ("bytes=100-0", s.StreamingBadRange),
        ("bytes=abc-", s.StreamingBadRange),
        ("bytes=-10-20", s.StreamingBadRange),
        ("bytes=5bc-600", s.StreamingBadRange),
        ("bytes=0-50,100-0", s.StreamingBadRange),  # terminal inverted range
        ("bytes=0-50,30-100", s.StreamingBadRange),  # don't support overlapping ranges
        ("bytes=0-30,30-100", s.StreamingBadRange),  # don't support overlapping ranges (note inclusive ranges)
        ("bytes=0-30,35-33,40-100", s.StreamingBadRange),  # non-terminal inverted range
        ("bytes=100000-", s.StreamingRangeNotSatisfiable),  # past end of file
        ("bytes=-100000", s.StreamingRangeNotSatisfiable),  # past end of file
    ],
)
def test_parse_range_header(range_header: str, exc: Type[Exception]):
    with pytest.raises(exc):
        s.parse_range_header(range_header, 10000)


@pytest.mark.asyncio()
async def test_file_streaming():
    stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, "bytes=0-")

    stream_contents = b""
    async for chunk in stream:
        stream_contents += chunk

    with open(SARS_COV_2_FASTA_PATH, "rb") as fh:
        fc = fh.read()

    assert fc == stream_contents
    file_length = len(fc)

    # ---

    stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, "bytes=0-", yield_content_length_as_first_8=True)
    content_length = int.from_bytes(await anext(stream), byteorder="big")
    assert content_length == file_length
    async for chunk in stream:
        assert isinstance(chunk, bytes)


with open(SARS_COV_2_FASTA_PATH, "rb") as fh:
    COVID_FASTA_BYTES = fh.read()


@pytest.mark.parametrize(
    "range_header,expected",
    [
        ("bytes=0-10", b">MN908947.3"),
        ("bytes=5-10", b"8947.3"),
        ("bytes=10-", COVID_FASTA_BYTES[10:]),
        ("bytes=0-2, 5-5", b">MN8"),
        ("bytes=0-2, 5-5, -5", b">MN8AAAAA\n"),
        ("bytes=-5", b"AAAAA\n"),
    ],
)
@pytest.mark.asyncio()
async def test_file_streaming_ranges(range_header: str, expected: bytes):
    stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, range_header)
    stream_contents = b""
    async for chunk in stream:
        stream_contents += chunk
    assert stream_contents == expected


@pytest.mark.asyncio()
async def test_file_streaming_range_errors():
    with pytest.raises(s.StreamingRangeNotSatisfiable):
        stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, "bytes=1000000000-")  # past EOF
        await anext(stream)

    with pytest.raises(s.StreamingRangeNotSatisfiable):
        stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, "bytes=0-10000000000")  # past EOF
        await anext(stream)

    with pytest.raises(s.StreamingBadRange):
        stream = s.stream_file(c.get_config(), SARS_COV_2_FASTA_PATH, "bytes=10000-5000")  # start > end
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, body=b"test page")

    # test that we get back content as expected
    stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {})
    assert (await anext(stream))[:9] == b"test page"

    # test that we can consume the entire stream
    async for chunk in stream:
        assert isinstance(chunk, bytes)

    # Test with content-length response
    aioresponse.get(HTTP_TEST_URI, body=b"test page", headers={"content-length": "9"})
    stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {}, yield_content_length_as_first_8=True)
    assert (await anext(stream)) == (9).to_bytes(8, byteorder="big")
    assert (await anext(stream))[:9] == b"test page"


@pytest.mark.asyncio()
async def test_http_streaming_416(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, body=b"Not Satisfiable")
    with pytest.raises(s.StreamingRangeNotSatisfiable):
        stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {"Range": "bytes=0-100000"})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_no_content_length(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, body=b"test page")  # doesn't have content-length header in response
    with pytest.raises(s.StreamingProxyingError):
        stream = s.stream_http(
            c.get_config(), HTTP_TEST_URI, {"Range": "bytes=0-100000"}, yield_content_length_as_first_8=True
        )
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_1(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(s.StreamingProxyingError):
        stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_2(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(s.StreamingProxyingError):
        _, stream = await s.stream_from_uri(c.get_config(), logger, HTTP_TEST_URI, None, False)
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_3(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(HTTPException):
        res = await s.generate_uri_streaming_response(
            c.get_config(),
            logger,
            HTTP_TEST_URI,
            None,
            "text/plain",
            False,
        )
        await anext(res.body_iterator)
