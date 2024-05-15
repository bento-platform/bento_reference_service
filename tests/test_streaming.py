import logging
import pytest

from aioresponses import aioresponses
from fastapi import HTTPException

from bento_reference_service import config as c, streaming as s

HTTP_TEST_URI = "https://test.local/file.txt"

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_http_streaming(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, body=b"test page")

    # test that we get back content as expected
    stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {})
    assert (await anext(stream))[:9] == b"test page"

    # test that we can consume the entire stream
    async for chunk in stream:
        assert isinstance(chunk, bytes)


@pytest.mark.asyncio()
async def test_http_streaming_404_1(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=404, body=b"Not Found")
    with pytest.raises(s.StreamingProxyingError):
        stream = s.stream_http(c.get_config(), HTTP_TEST_URI, {})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_2(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=404, body=b"Not Found")
    with pytest.raises(s.StreamingProxyingError):
        _, stream = await s.stream_from_uri(c.get_config(), logger, HTTP_TEST_URI, None, False)
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_3(aioresponse: aioresponses):
    aioresponse.get(HTTP_TEST_URI, status=404, body=b"Not Found")
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
