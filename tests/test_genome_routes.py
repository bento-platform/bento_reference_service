import pytest

from aioresponses import aioresponses
from fastapi import status
from fastapi.testclient import TestClient
from httpx import Response

from .shared_data import (
    SARS_COV_2_GENOME_ID,
    SARS_COV_2_FASTA_PATH,
    SARS_COV_2_FAI_PATH,
    SARS_COV_2_GFF3_GZ_PATH,
    SARS_COV_2_GFF3_GZ_TBI_PATH,
    TEST_GENOME_OF_FILE_URIS,
)

# all tests are async so that db_cleanup (an async fixture) properly works. not sure why it's this way.

pytestmark = pytest.mark.asyncio()


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


def create_genome_with_permissions(test_client: TestClient, aioresponse: aioresponses) -> Response:
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.post("/genomes", json=TEST_GENOME_OF_FILE_URIS, headers={"Authorization": "Token bearer"})
    return res


async def test_genome_create(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    res = test_client.post("/genomes", json=TEST_GENOME_OF_FILE_URIS)
    assert res.status_code == status.HTTP_401_UNAUTHORIZED

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[False]]})
    res = test_client.post("/genomes", json=TEST_GENOME_OF_FILE_URIS, headers={"Authorization": "Token bearer"})
    assert res.status_code == status.HTTP_403_FORBIDDEN

    res = create_genome_with_permissions(test_client, aioresponse)
    assert res.status_code == status.HTTP_201_CREATED

    res = create_genome_with_permissions(test_client, aioresponse)  # test we cannot recreate
    assert res.status_code == status.HTTP_400_BAD_REQUEST


async def test_genome_detail_endpoints(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # setup: create genome  TODO: fixture
    create_genome_with_permissions(test_client, aioresponse)

    # tests

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_200_OK

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/contigs")
    assert res.status_code == status.HTTP_200_OK

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/contigs/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_200_OK

    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/contigs/does-not-exist")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    #  - FASTA
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}.fa")
    assert res.status_code == status.HTTP_200_OK
    assert res.headers.get("Content-Type") == "text/x-fasta; charset=utf-8"
    with open(SARS_COV_2_FASTA_PATH, "rb") as fh:
        assert res.content == fh.read()

    #  - FASTA range header
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}.fa", headers={"Range": "bytes=0-0"})
    assert res.status_code == status.HTTP_206_PARTIAL_CONTENT
    assert res.headers.get("Content-Type") == "text/x-fasta; charset=utf-8"
    assert res.content == b">"

    # - FAI
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}.fa.fai")
    assert res.status_code == status.HTTP_200_OK
    assert res.headers.get("Content-Type") == "text/plain; charset=utf-8"
    with open(SARS_COV_2_FAI_PATH, "rb") as fh:
        assert res.content == fh.read()

    # - FAI range header
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}.fa.fai", headers={"Range": "bytes=0-0"})
    assert res.status_code == status.HTTP_206_PARTIAL_CONTENT
    assert res.headers.get("Content-Type") == "text/plain; charset=utf-8"
    assert res.content == b"M"

    # - Feature GFF3
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz")
    assert res.status_code == status.HTTP_200_OK
    with open(SARS_COV_2_GFF3_GZ_PATH, "rb") as fh:
        assert res.content == fh.read()

    # - Feature GFF3 TBI
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz.tbi")
    assert res.status_code == status.HTTP_200_OK
    with open(SARS_COV_2_GFF3_GZ_TBI_PATH, "rb") as fh:
        assert res.content == fh.read()


async def test_genome_delete(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # setup: create genome  TODO: fixture
    create_genome_with_permissions(test_client, aioresponse)

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers={"Authorization": "Token bearer"})
    assert res.status_code == status.HTTP_204_NO_CONTENT

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers={"Authorization": "Token bearer"})
    assert res.status_code == status.HTTP_404_NOT_FOUND  # already deleted

    res = create_genome_with_permissions(test_client, aioresponse)  # test we can re-create
    assert res.status_code == status.HTTP_201_CREATED

    # test that we cannot delete with no token
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_401_UNAUTHORIZED

    # test that we cannot delete with no permission
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[False]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers={"Authorization": "Token bearer"})
    assert res.status_code == status.HTTP_403_FORBIDDEN


async def test_genome_feature_ingest(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    # setup: create genome  TODO: fixture
    create_genome_with_permissions(test_client, aioresponse)

    hs = {"Authorization": "Token bearer"}

    # Test we can create a task for ingesting features

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]}, repeat=True)

    with open(SARS_COV_2_GFF3_GZ_PATH, "rb") as gff3_fh, open(SARS_COV_2_GFF3_GZ_TBI_PATH, "rb") as tbi_fh:
        res = test_client.put(
            f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz",
            files={"gff3_gz": gff3_fh, "gff3_gz_tbi": tbi_fh},
            headers=hs,
        )

    assert res.status_code == status.HTTP_202_ACCEPTED
    data = res.json()
    assert "task" in data
    task_id = data["task"].split("/")[-1]

    # Test we can access the task and that it eventually succeeds

    finished: bool = False
    task_status: str = ""
    task_msg: str = ""
    while not finished:
        res = test_client.get(f"/tasks/{task_id}", headers=hs)
        assert res.status_code == status.HTTP_200_OK
        rd = res.json()
        task_status = rd["status"]
        task_msg = rd["message"]
        finished = task_status in {"success", "error"}

    assert task_status == "success"
    assert task_msg == "ingested 49 features"
