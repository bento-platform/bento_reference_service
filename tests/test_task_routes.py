import pytest
from aiointercept import aiointercept
from fastapi import status
from fastapi.testclient import TestClient

from bento_reference_service.db import Database

from .shared_data import SARS_COV_2_GENOME_ID, TEST_GENOME_SARS_COV_2, AUTHORIZATION_HEADER
from .shared_functions import create_genome_with_permissions


@pytest.mark.asyncio()
async def test_task_create_no_genome(test_client: TestClient, aio: aiointercept, db_cleanup):
    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.post("/tasks", json={"genome_id": "DNE", "kind": "ingest_features"}, headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_400_BAD_REQUEST  # 400: no genome
    err = res.json()
    assert err["errors"][0]["message"] == "Genome with ID DNE not found."


@pytest.mark.asyncio()
async def test_task_routes(test_client: TestClient, aio: aiointercept, db: Database, db_cleanup):
    # prerequesite: set up a genome
    create_genome_with_permissions(test_client, aio, TEST_GENOME_SARS_COV_2)

    # prerequesite: initialize the database for the web app + validate there aren't any tasks
    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get("/tasks", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_200_OK
    rd = res.json()
    assert len(rd) == 0

    # prerequesite: set up a dummy task
    await db.create_task(SARS_COV_2_GENOME_ID, "ingest_features")

    # make sure the task now shows up in the list of tasks in the initial state
    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get("/tasks", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_200_OK
    rd = res.json()
    assert len(rd) == 1
    assert rd[0]["genome_id"] == SARS_COV_2_GENOME_ID
    assert rd[0]["status"] == "queued"

    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get(f"/tasks/{rd[0]['id']}", headers=AUTHORIZATION_HEADER)
    rd2 = res.json()
    assert rd[0] == rd2

    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get("/tasks/0", headers=AUTHORIZATION_HEADER)
    assert res.status_code == status.HTTP_404_NOT_FOUND
