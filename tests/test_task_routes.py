import pytest
from aioresponses import aioresponses
from fastapi.testclient import TestClient

from bento_reference_service.db import Database

from .shared_data import SARS_COV_2_GENOME_ID, TEST_GENOME_SARS_COV_2, AUTHORIZATION_HEADER
from .shared_functions import create_genome_with_permissions


@pytest.mark.asyncio()
async def test_task_routes(test_client: TestClient, aioresponse: aioresponses, db: Database, db_cleanup):
    # prerequesite: set up a genome
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)

    # prerequesite: initialize the database for the web app + validate there aren't any tasks
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get("/tasks", headers=AUTHORIZATION_HEADER)
    assert res.status_code == 200
    rd = res.json()
    assert len(rd) == 0

    # prerequesite: set up a dummy task
    await db.create_task(SARS_COV_2_GENOME_ID, "ingest_features")

    # make sure the task now shows up in the list of tasks in the initial state
    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get("/tasks", headers=AUTHORIZATION_HEADER)
    assert res.status_code == 200
    rd = res.json()
    assert len(rd) == 1
    assert rd[0]["genome_id"] == SARS_COV_2_GENOME_ID
    assert rd[0]["status"] == "queued"

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get(f"/tasks/{rd[0]['id']}", headers=AUTHORIZATION_HEADER)
    rd2 = res.json()
    assert rd[0] == rd2

    aioresponse.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.get(f"/tasks/0", headers=AUTHORIZATION_HEADER)
    assert res.status_code == 404
