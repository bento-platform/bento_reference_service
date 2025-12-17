import pytest

from aioresponses import aioresponses
from fastapi import status
from fastapi.testclient import TestClient
from httpx import Response

from bento_reference_service.models import Genome

from .shared_data import (
    SARS_COV_2_GENOME_ID,
    SARS_COV_2_FASTA_PATH,
    SARS_COV_2_FAI_PATH,
    SARS_COV_2_GFF3_GZ_PATH,
    SARS_COV_2_GFF3_GZ_TBI_PATH,
    TEST_GENOME_SARS_COV_2,
    TEST_GENOME_SARS_COV_2_OBJ,
    TEST_GENOME_HG38_CHR1_F100K,
    TEST_GENOME_HG38_CHR1_F100K_OBJ,
    AUTHORIZATION_HEADER,
)
from .shared_functions import create_genome_with_permissions

# all tests are async so that db_cleanup (an async fixture) properly works. not sure why it's this way.

pytestmark = pytest.mark.asyncio()


async def test_genome_list(test_client: TestClient):
    res = test_client.get("/genomes")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list

    res = test_client.get("/genomes?response_format=id_list")
    assert res.status_code == status.HTTP_200_OK
    assert res.content == b"[]"  # empty json list


@pytest.mark.parametrize(
    "endpoint",
    (
        "/genomes/hg19",
        "/genomes/hg19/contigs",
        "/genomes/hg19/contigs/chr1",
        "/genomes/hg19.fa",
        "/genomes/hg19.fa.fai",
        "/genomes/hg19/features",
        "/genomes/hg19/features/1",
        "/genomes/hg19/features.gff3.gz",
        "/genomes/hg19/features.gff3.gz.tbi",
        "/genomes/hg19/igv-js-features",
    ),
)
async def test_get_404s_with_no_genomes(test_client: TestClient, endpoint: str):
    assert test_client.get(endpoint).status_code == status.HTTP_404_NOT_FOUND


def create_covid_genome_with_permissions(test_client: TestClient, aioresponse: aioresponses) -> Response:
    return create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)


def create_hg38_subset_genome_with_permissions(test_client: TestClient, aioresponse: aioresponses) -> Response:
    return create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_HG38_CHR1_F100K)


async def test_genome_create(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    res = test_client.post("/genomes", json=TEST_GENOME_SARS_COV_2)
    assert res.status_code == status.HTTP_401_UNAUTHORIZED

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[False]]})
    res = test_client.post("/genomes", json=TEST_GENOME_SARS_COV_2, headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_403_FORBIDDEN

    # SARS-CoV-2

    res = create_covid_genome_with_permissions(test_client, aioresponse)
    assert res.status_code == status.HTTP_201_CREATED

    res = create_covid_genome_with_permissions(test_client, aioresponse)  # test we cannot recreate
    assert res.status_code == status.HTTP_400_BAD_REQUEST

    # - test list has one entry
    res = test_client.get("/genomes")
    assert res.status_code == status.HTTP_200_OK
    assert len(res.json()) == 1

    # hg38 subset

    res = create_hg38_subset_genome_with_permissions(test_client, aioresponse)
    assert res.status_code == status.HTTP_201_CREATED

    res = create_hg38_subset_genome_with_permissions(test_client, aioresponse)  # test we cannot recreate
    assert res.status_code == status.HTTP_400_BAD_REQUEST

    # - test list has two entries
    res = test_client.get("/genomes")
    assert res.status_code == status.HTTP_200_OK
    assert len(res.json()) == 2


async def test_genome_create_taxon_validation(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    res = create_genome_with_permissions(
        test_client, aioresponse, {**TEST_GENOME_SARS_COV_2, "taxon": {"id": "asdf:123", "label": "covid"}}
    )
    assert res.status_code == status.HTTP_400_BAD_REQUEST
    res_json = res.json()
    del res_json["timestamp"]
    assert res_json == {
        "code": status.HTTP_400_BAD_REQUEST,
        "message": "Bad Request",
        "errors": [{"message": r"body.taxon.id: String should match pattern '^NCBITaxon:[a-zA-Z0-9.\-_]+$'"}]
    }


async def test_genome_detail_endpoints(test_client: TestClient, sars_cov_2_genome):
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


async def test_genome_without_gff3_and_then_patch(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    covid_genome_without_gff3 = {**TEST_GENOME_SARS_COV_2}
    del covid_genome_without_gff3["gff3_gz"]
    del covid_genome_without_gff3["gff3_gz_tbi"]

    # ingest a genome without GFF3/TBI URIs (we'll add them in later)
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.post("/genomes", json=covid_genome_without_gff3, headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_201_CREATED

    # check that the genome ingested
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_200_OK

    # check that we get 404s for the gff3 files, since we haven't ingested them yet
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz")
    assert res.status_code == status.HTTP_404_NOT_FOUND
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz.tbi")
    assert res.status_code == status.HTTP_404_NOT_FOUND

    # update the genome with GFF3/TBI URIs
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.patch(
        f"/genomes/{SARS_COV_2_GENOME_ID}",
        json={
            "gff3_gz": TEST_GENOME_SARS_COV_2["gff3_gz"],
            "gff3_gz_tbi": TEST_GENOME_SARS_COV_2["gff3_gz_tbi"],
        },
        headers=AUTHORIZATION_HEADER,
    )
    assert res.status_code == status.HTTP_204_NO_CONTENT

    # check that we can now access the GFF3/TBI
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz")
    assert res.status_code == status.HTTP_200_OK
    res = test_client.get(f"/genomes/{SARS_COV_2_GENOME_ID}/features.gff3.gz.tbi")
    assert res.status_code == status.HTTP_200_OK


async def test_genome_delete(test_client: TestClient, sars_cov_2_genome, aioresponse: aioresponses):
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_204_NO_CONTENT

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_404_NOT_FOUND  # already deleted

    res = create_covid_genome_with_permissions(test_client, aioresponse)  # test we can re-create
    assert res.status_code == status.HTTP_201_CREATED

    # test that we cannot delete with no token
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}")
    assert res.status_code == status.HTTP_401_UNAUTHORIZED

    # test that we cannot delete with no permission
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[False]]})
    res = test_client.delete(f"/genomes/{SARS_COV_2_GENOME_ID}", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_403_FORBIDDEN


def _file_uri_to_path(uri: str) -> str:
    return uri.removeprefix("file://")


def _put_genome_features(test_client: TestClient, genome: Genome) -> Response:
    return test_client.post(
        "/tasks",
        json={"genome_id": genome.id, "kind": "ingest_features"},
        headers=AUTHORIZATION_HEADER,
    )


def _test_ingest_genome_features(test_client: TestClient, genome: Genome, expected_features: int):
    # Test we can create a task for ingesting features

    res = _put_genome_features(test_client, genome)

    assert res.status_code == status.HTTP_201_CREATED
    task_id = res.json()["id"]

    # Test we can access the task and that it eventually succeeds

    finished: bool = False
    task_status: str = ""
    task_msg: str = ""
    while not finished:
        res = test_client.get(f"/tasks/{task_id}", headers=AUTHORIZATION_HEADER)
        assert res.status_code == status.HTTP_200_OK
        rd = res.json()
        task_status = rd["status"]
        task_msg = rd["message"]
        finished = task_status in {"success", "error"}

    assert task_status == "success"
    assert task_msg == f"ingested {expected_features} features"


@pytest.mark.parametrize(
    "genome,expected_features", [(TEST_GENOME_SARS_COV_2_OBJ, 49), (TEST_GENOME_HG38_CHR1_F100K_OBJ, 13)]
)
async def test_genome_feature_ingest(
    test_client: TestClient, aioresponse: aioresponses, db_cleanup, genome: Genome, expected_features: int
):
    # setup: create genome
    create_genome_with_permissions(test_client, aioresponse, genome.model_dump(mode="json"))

    # Test we cannot ingest without permissions
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[False]]})
    res = _put_genome_features(test_client, genome)
    assert res.status_code == status.HTTP_403_FORBIDDEN

    # Test we can ingest features

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]}, repeat=True)
    _test_ingest_genome_features(test_client, genome, expected_features)

    # Test we can delete
    res = test_client.delete(f"/genomes/{genome.id}/features", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_204_NO_CONTENT

    # Test we can ingest again
    _test_ingest_genome_features(test_client, genome, expected_features)

    # Test we can delete again

    res = test_client.delete(f"/genomes/{genome.id}/features", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_204_NO_CONTENT


async def test_genome_feature_endpoints(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    genome = TEST_GENOME_SARS_COV_2_OBJ
    expected_features = 49

    # setup: create genome
    create_genome_with_permissions(test_client, aioresponse, genome.model_dump(mode="json"))

    # setup: ingest features
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]}, repeat=True)
    _test_ingest_genome_features(test_client, genome, expected_features)

    # Test we can query genome features
    sr = test_client.get(f"/genomes/{genome.id}/feature_types")
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert sum(srd.values()) == expected_features

    # Test we can query genome features

    #  - regular expression
    sr = test_client.get(f"/genomes/{genome.id}/features", params={"q": "ENSSASP00005000003"})
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert len(srd["results"]) == 1
    assert srd["pagination"]["total"] == 1
    assert isinstance(srd.get("time"), float)
    assert srd["time"] < 0.2  # this is a very basic operation on a small dataset and should be fast.
    q_feature = srd["results"][0]

    #  - fuzzy search
    sr = test_client.get(f"/genomes/{genome.id}/features", params={"q": "ENSSASP00005000003", "q_fzy": "true"})
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert len(srd["results"]) == 10  # fuzzy search yields many results

    # Test we can filter genome features (ID used as name)
    sr = test_client.get(f"/genomes/{genome.id}/features", params={"name": "CDS:ENSSASP00005000003"})
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert len(srd["results"]) == 1
    assert srd["pagination"]["total"] == 1

    # Test we can list genome features - we get back the first 10
    sr = test_client.get(f"/genomes/{genome.id}/features")
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert len(srd["results"]) == 10
    assert srd["pagination"]["offset"] == 0
    assert srd["pagination"]["total"] == 10

    # Test we can get a feature by ID
    sr = test_client.get(f"/genomes/{genome.id}/features/CDS:ENSSASP00005000003")
    assert sr.status_code == status.HTTP_200_OK
    assert sr.json()["feature_id"] == "CDS:ENSSASP00005000003"

    # Test we can get a feature by ID
    sr = test_client.get(f"/genomes/{genome.id}/features/does-not-exist")
    assert sr.status_code == status.HTTP_404_NOT_FOUND

    # Test we can do an IGV.js search
    sr = test_client.get(f"/genomes/{genome.id}/igv-js-features", params={"q": "ENSSASP00005000003"})
    assert sr.status_code == status.HTTP_200_OK
    srd = sr.json()
    assert len(srd) == 1
    assert srd[0]["chromosome"] == q_feature["contig_name"]
    assert srd[0]["start"] == q_feature["entries"][0]["start_pos"]
    assert srd[0]["end"] == q_feature["entries"][0]["end_pos"]
