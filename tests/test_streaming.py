import logging
import pytest

from aioresponses import aioresponses
from bento_lib.streaming import exceptions as se
from fastapi import HTTPException, status

from bento_reference_service import config as c, drs as d, streaming as s

HTTP_TEST_URI = "https://test.local/file.txt"

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_uri_streaming_bad_uri():
    config = c.get_config()
    with pytest.raises(se.StreamingBadURI):
        await s.stream_from_uri(config, d.get_drs_resolver(config), logger, "http://[.com", None, False)


@pytest.mark.asyncio()
async def test_uri_streaming_bad_scheme():
    config = c.get_config()
    with pytest.raises(se.StreamingUnsupportedURIScheme):
        await s.stream_from_uri(config, d.get_drs_resolver(config), logger, "asdf://example.org", None, False)


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
    stream = s.stream_http(
        c.get_config(), HTTP_TEST_URI, {}, yield_status_as_first_2=True, yield_content_length_as_next_8=True
    )
    assert int.from_bytes(await anext(stream), "big") == status.HTTP_200_OK
    assert (await anext(stream)) == (9).to_bytes(8, byteorder="big")
    assert (await anext(stream))[:9] == b"test page"


@pytest.mark.asyncio()
async def test_http_streaming_416(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, body=b"Not Satisfiable")
    with pytest.raises(se.StreamingRangeNotSatisfiable):
        stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {"Range": "bytes=0-100000"})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_no_content_length(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, body=b"test page")  # doesn't have content-length header in response
    with pytest.raises(se.StreamingProxyingError):
        stream = s.stream_http(
            c.get_config(), HTTP_TEST_URI, {"Range": "bytes=0-100000"}, yield_content_length_as_next_8=True
        )
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_1(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(se.StreamingProxyingError):
        stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_2(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(se.StreamingProxyingError):
        config = c.get_config()
        _, _, stream = await s.stream_from_uri(config, d.get_drs_resolver(config), logger, HTTP_TEST_URI, None, False)
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_3(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(HTTPException):
        config = c.get_config()
        res = await s.generate_uri_streaming_response(
            config,
            d.get_drs_resolver(config),
            logger,
            HTTP_TEST_URI,
            None,
            "text/plain",
            False,
        )
        await anext(res.body_iterator)
