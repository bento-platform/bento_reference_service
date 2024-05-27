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


def test_refget_sequence_full(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # TODO: fixture
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)

    test_contig = TEST_GENOME_SARS_COV_2['contigs'][0]

    # Load COVID contig bytes
    rf = pysam.FastaFile(str(SARS_COV_2_FASTA_PATH))
    seq = rf.fetch(test_contig["name"]).encode("ascii")

    # COVID genome should be small enough to fit in the default max-size Refget response, yielding a 200 (not a 206):
    res = test_client.get(f"/sequence/{test_contig['md5']}", headers=HEADERS_ACCEPT_PLAIN)
    assert res.status_code == status.HTTP_200_OK
    assert res.headers["Content-Type"] == "text/vnd.ga4gh.refget.v2.0.0+plain; charset=us-ascii"
    assert res.content == seq
