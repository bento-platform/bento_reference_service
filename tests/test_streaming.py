import logging

import pytest

from fastapi import HTTPException

from bento_reference_service import config as c
from bento_reference_service import streaming as s

HTTP_TEST_URI_1 = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/md5sum.txt"

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_http_streaming():
    # test that we get back content as expected
    stream = s.stream_http(c.get_config(), HTTP_TEST_URI_1, {})
    assert (await anext(stream))[:32] == b"f069c41e7cc8c2d3a7655cbb2d4186b8"  # MD5 sum for chr1

    # test that we can consume the entire stream
    async for chunk in stream:
        assert isinstance(chunk, bytes)


@pytest.mark.asyncio()
async def test_http_streaming_404_1():
    with pytest.raises(s.StreamingProxyingError):
        stream = s.stream_http(c.get_config(), "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/DOES_NOT_EXIST", {})
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_2():
    with pytest.raises(s.StreamingProxyingError):
        _, stream = await s.stream_from_uri(
            c.get_config(), logger, "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/DOES_NOT_EXIST", None, False
        )
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_3():
    with pytest.raises(HTTPException):
        res = await s.generate_uri_streaming_response(
            c.get_config(),
            logger,
            "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/DOES_NOT_EXIST",
            None,
            "text/plain",
            False,
        )
        await anext(res.body_iterator)
