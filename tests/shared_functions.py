from aiointercept import aiointercept
from fastapi.testclient import TestClient
from httpx import Response

from .shared_data import AUTHORIZATION_HEADER

__all__ = ["create_genome_with_permissions"]


def create_genome_with_permissions(test_client: TestClient, aio: aiointercept, genome: dict) -> Response:
    aio.post("https://authz.local/policy/evaluate", payload={"result": [[True]]})
    res = test_client.post("/genomes", json=genome, headers=AUTHORIZATION_HEADER)
    return res
