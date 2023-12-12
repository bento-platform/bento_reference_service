import pytest

from fastapi import status
from fastapi.testclient import TestClient

from .shared_data import SARS_COV_2_GENOME_ID, TEST_GENOME_OF_FILE_URIS

# all tests are async so that db_cleanup (an async fixture) properly works. not sure why it's this way.

pytestmark = pytest.mark.asyncio(scope="module")


async def test_genome_list(test_client: TestClient):
    res = test_client.get("/genomes")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list

    res = test_client.get("/genomes?response_format=id_list")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list


async def test_404s_with_no_genomes(test_client: TestClient):
    res = test_client.get("/genomes/hg19")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    res = test_client.get("/genomes/hg19/contigs")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    res = test_client.get("/genomes/hg19/contigs/chr1")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    res = test_client.get("/genomes/hg19.fa")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    res = test_client.get("/genomes/hg19.fa.fai")
    assert res.status_code == status.HTTP_404_NOT_FOUND


async def test_genome_post(test_client: TestClient, db_cleanup):
    res = test_client.post("/genomes", json=TEST_GENOME_OF_FILE_URIS)
    assert res.status_code == status.HTTP_201_CREATED


async def test_genome_detail_endpoints(test_client: TestClient, db_cleanup):
    # setup: create genome  TODO: fixture
    res = test_client.post("/genomes", json=TEST_GENOME_OF_FILE_URIS)
    assert res.status_code == status.HTTP_201_CREATED

    # tests

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_200_OK

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}.fa")
    assert res.status_code == status.HTTP_200_OK
    assert res.headers.get("Content-Type") == "text/x-fasta; charset=utf-8"
