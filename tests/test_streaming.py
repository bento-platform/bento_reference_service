import logging
import pytest

from aioresponses import aioresponses
from bento_lib.drs.resolver import DrsResolver
from bento_lib.streaming import exceptions as se
from fastapi import HTTPException, status

from bento_reference_service import config as c, streaming as s

from .shared_data import TEST_DRS_REPLY_NO_ACCESS, TEST_DRS_REPLY

HTTP_TEST_URI = "https://test.local/file.txt"

logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
async def test_drs_bytes_url_from_uri(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get("https://example.org/ga4gh/drs/v1/objects/abc", payload=TEST_DRS_REPLY)
    assert (
        await s.drs_bytes_url_from_uri(config, drs_resolver, logger, "drs://example.org/abc")
        == TEST_DRS_REPLY["access_methods"][1]["access_url"]["url"]
    )


@pytest.mark.asyncio()
async def test_drs_bytes_url_from_uri_not_found(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get(
        "https://example.org/ga4gh/drs/v1/objects/abc",
        status=status.HTTP_404_NOT_FOUND,
        payload={"message": "Not Found"},
    )

    with pytest.raises(HTTPException) as e:
        await s.drs_bytes_url_from_uri(config, drs_resolver, logger, "drs://example.org/abc")

    assert e.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Not Found error" in e.value.detail


@pytest.mark.asyncio()
async def test_drs_bytes_url_from_uri_500(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get(
        "https://example.org/ga4gh/drs/v1/objects/abc",
        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        payload={"message": "Internal Server Error"},
    )

    with pytest.raises(HTTPException) as e:
        await s.drs_bytes_url_from_uri(config, drs_resolver, logger, "drs://example.org/abc")

    assert e.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "while accessing DRS record" in e.value.detail


@pytest.mark.asyncio()
async def test_drs_bytes_url_from_uri_no_access(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get("https://example.org/ga4gh/drs/v1/objects/abc", payload=TEST_DRS_REPLY_NO_ACCESS)

    with pytest.raises(HTTPException) as e:
        await s.drs_bytes_url_from_uri(config, drs_resolver, logger, "drs://example.org/abc")

    assert e.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "HTTPS access method" in e.value.detail


@pytest.mark.asyncio()
async def test_uri_streaming_bad_uri(config: c.Config, drs_resolver: DrsResolver):
    with pytest.raises(se.StreamingBadURI):
        await s.stream_from_uri(config, drs_resolver, logger, "http://[.com", None, False)


@pytest.mark.asyncio()
async def test_uri_streaming_bad_scheme(config: c.Config, drs_resolver: DrsResolver):
    with pytest.raises(se.StreamingUnsupportedURIScheme):
        await s.stream_from_uri(config, drs_resolver, logger, "asdf://example.org", None, False)


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
async def test_http_streaming_404_2(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(se.StreamingProxyingError):
        _, _, stream = await s.stream_from_uri(config, drs_resolver, logger, HTTP_TEST_URI, None, False)
        await anext(stream)


@pytest.mark.asyncio()
async def test_http_streaming_404_3(aioresponse: aioresponses, config: c.Config, drs_resolver: DrsResolver):
    aioresponse.get(HTTP_TEST_URI, status=status.HTTP_404_NOT_FOUND, body=b"Not Found")
    with pytest.raises(HTTPException):
        config = c.get_config()
        res = await s.generate_uri_streaming_response(
            config,
            drs_resolver,
            logger,
            HTTP_TEST_URI,
            None,
            "text/plain",
            False,
        )
        await anext(res.body_iterator)
