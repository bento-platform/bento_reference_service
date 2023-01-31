from fastapi import APIRouter
from typing import List

__all__ = ["ingest_router"]

ingest_router = APIRouter()


@ingest_router.post("/private/ingest")
async def genomes_ingest() -> List[dict]:
    # Weird endpoint for now - old Bento ingest style backwards compatibility
    raise NotImplementedError()  # TODO
