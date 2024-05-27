import sys

from fastapi import status
from fastapi.testclient import TestClient

REFGET_2_0_0_TYPE = {"group": "org.ga4gh", "artifact": "refget", "version": "2.0.0"}


def test_refget_service_info(test_client: TestClient, db_cleanup):
    res = test_client.get("/sequence/service-info")
    rd = res.json()

    sys.stderr.write(str(rd) + "\n")

    assert res.status_code == status.HTTP_200_OK

    assert "id" in rd
    assert "name" in rd
    assert rd["type"] == REFGET_2_0_0_TYPE
    assert "refget" in rd
    assert "circular_supported" in rd["refget"]
    assert "subsequence_limit" in rd["refget"]
    assert "algorithms" in rd["refget"]
    assert "identifier_types" in rd["refget"]

    res = test_client.get("/sequence/service-info", headers={"Accept": "text/plain"})
    assert res.status_code == status.HTTP_406_NOT_ACCEPTABLE
