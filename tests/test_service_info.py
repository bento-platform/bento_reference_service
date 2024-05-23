from fastapi import status
from fastapi.testclient import TestClient


def test_service_info(test_client: TestClient):
    res = test_client.get("/service-info")
    assert res.status_code == status.HTTP_200_OK
