from fastapi import status
from fastapi.testclient import TestClient


def test_genome_list(test_client: TestClient):
    res = test_client.get("/genomes")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list

    res = test_client.get("/genomes?response_format=id_list")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list


def test_404s_with_no_genomes(test_client: TestClient):
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
