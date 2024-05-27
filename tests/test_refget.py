import pysam

from aioresponses import aioresponses
from fastapi import status
from fastapi.testclient import TestClient

from .shared_data import SARS_COV_2_FASTA_PATH, TEST_GENOME_SARS_COV_2
from .shared_functions import create_genome_with_permissions


REFGET_2_0_0_TYPE = {"group": "org.ga4gh", "artifact": "refget", "version": "2.0.0"}

HEADERS_ACCEPT_PLAIN = {"Accept": "text/plain"}


def test_refget_service_info(test_client: TestClient, db_cleanup):
    res = test_client.get("/sequence/service-info")
    rd = res.json()

    assert res.status_code == status.HTTP_200_OK

    assert "id" in rd
    assert "name" in rd
    assert rd["type"] == REFGET_2_0_0_TYPE
    assert "refget" in rd
    assert "circular_supported" in rd["refget"]
    assert "subsequence_limit" in rd["refget"]
    assert "algorithms" in rd["refget"]
    assert "identifier_types" in rd["refget"]

    res = test_client.get("/sequence/service-info", headers=HEADERS_ACCEPT_PLAIN)
    assert res.status_code == status.HTTP_406_NOT_ACCEPTABLE


def test_refget_sequence_not_found(test_client: TestClient, db_cleanup):
    res = test_client.get(f"/sequence/does-not-exist", headers=HEADERS_ACCEPT_PLAIN)
    assert res.status_code == status.HTTP_404_NOT_FOUND


def test_refget_sequence_invalid_requests(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # TODO: fixture
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)
    test_contig = TEST_GENOME_SARS_COV_2["contigs"][0]

    # ------------------------------------------------------------------------------------------------------------------

    # cannot return HTML
    res = test_client.get(f"/sequence/{test_contig['md5']}", headers={"Accept": "text/html"})
    assert res.status_code == status.HTTP_406_NOT_ACCEPTABLE
    assert res.content == b"Not Acceptable"

    # cannot have start > end
    res = test_client.get(
        f"/sequence/{test_contig['md5']}", params={"start": 5, "end": 1}, headers=HEADERS_ACCEPT_PLAIN
    )
    assert res.status_code == status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE
    assert res.content == b"Range Not Satisfiable"

    # start > contig length (by 1 base, since it's 0-based)
    res = test_client.get(
        f"/sequence/{test_contig['md5']}", params={"start": test_contig["length"]}, headers=HEADERS_ACCEPT_PLAIN
    )
    assert res.status_code == status.HTTP_400_BAD_REQUEST
    assert res.content == b"Bad Request"

    # end > contig length (by 1 base, since it's 0-based exclusive)
    res = test_client.get(
        f"/sequence/{test_contig['md5']}", params={"end": test_contig["length"] + 1}, headers=HEADERS_ACCEPT_PLAIN
    )
    assert res.status_code == status.HTTP_400_BAD_REQUEST
    assert res.content == b"Bad Request"

    # bad range header
    res = test_client.get(f"/sequence/{test_contig['md5']}", headers={"Range": "dajkshfasd", **HEADERS_ACCEPT_PLAIN})
    assert res.status_code == status.HTTP_400_BAD_REQUEST
    assert res.content == b"Bad Request"


def test_refget_sequence_full(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # TODO: fixture
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)

    test_contig = TEST_GENOME_SARS_COV_2["contigs"][0]

    # Load COVID contig bytes
    rf = pysam.FastaFile(str(SARS_COV_2_FASTA_PATH))
    seq = rf.fetch(test_contig["name"]).encode("ascii")

    # ------------------------------------------------------------------------------------------------------------------

    # COVID genome should be small enough to fit in the default max-size Refget response, yielding a 200 (not a 206):

    spec_content_type = "text/vnd.ga4gh.refget.v2.0.0+plain; charset=us-ascii"

    res = test_client.get(f"/sequence/{test_contig['md5']}", headers=HEADERS_ACCEPT_PLAIN)
    assert res.status_code == status.HTTP_200_OK
    assert res.headers["Content-Type"] == spec_content_type
    assert res.content == seq

    # Range header starting at 0 should get the whole sequence as well

    res = test_client.get(f"/sequence/{test_contig['md5']}", headers={"Range": "bytes=0-", **HEADERS_ACCEPT_PLAIN})
    assert res.status_code == status.HTTP_206_PARTIAL_CONTENT
    assert res.headers["Content-Type"] == spec_content_type
    assert res.content == seq


def test_refget_sequence_partial(test_client, aioresponse: aioresponses, db_cleanup):
    # TODO: fixture
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)

    test_contig = TEST_GENOME_SARS_COV_2["contigs"][0]
    seq_url = f"/sequence/{test_contig['md5']}"

    # Load COVID contig bytes
    rf = pysam.FastaFile(str(SARS_COV_2_FASTA_PATH))
    seq = rf.fetch(test_contig["name"]).encode("ascii")

    # ------------------------------------------------------------------------------------------------------------------

    # The following three responses should be equivalent except for status codes:

    def _check_first_10(r, sc, ar="none"):
        assert r.status_code == sc
        assert r.headers["Accept-Ranges"] == ar
        assert r.headers["Content-Length"] == "10"
        assert r.content == seq[:10]

    res = test_client.get(seq_url, params={"start": "0", "end": "10"}, headers=HEADERS_ACCEPT_PLAIN)
    _check_first_10(res, status.HTTP_200_OK)

    res = test_client.get(seq_url, params={"end": "10"}, headers=HEADERS_ACCEPT_PLAIN)
    _check_first_10(res, status.HTTP_200_OK)

    # range - end is inclusive:
    res = test_client.get(seq_url, headers={"Range": "bytes=0-9", **HEADERS_ACCEPT_PLAIN})
    _check_first_10(res, status.HTTP_206_PARTIAL_CONTENT, "bytes")

    # ---

    res = test_client.get(seq_url, params={"start": "10"}, headers=HEADERS_ACCEPT_PLAIN)
    assert res.status_code == status.HTTP_200_OK
    assert res.content == seq[10:]

    res = test_client.get(seq_url, headers={"Range": "bytes=10-", **HEADERS_ACCEPT_PLAIN})
    assert res.status_code == status.HTTP_206_PARTIAL_CONTENT
    assert res.content == seq[10:]
