from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from bento_reference_service import tables as t
from bento_reference_service.genomes import get_genomes

# from .data_types import DATA_TYPE_GENOME


__all__ = [
    "table_router"
]


table_router = APIRouter(prefix="/tables")


async def _check_table_id(table_id: str):
    if table_id != (await t.get_table_id()):
        raise HTTPException(status_code=404, detail=f"table with ID {table_id} not found")


@table_router.get("")
async def get_tables():
    # TODO: require data-type query arg
    return JSONResponse([await t.get_table()])


@table_router.get("/{table_id}")
async def get_table(table_id: str):
    await _check_table_id(table_id)
    return await t.get_table()


@table_router.get("/{table_id}/summary")
async def get_table_summary(table_id: str):
    await _check_table_id(table_id)
    return {
        "count": len([g async for g in get_genomes()]),  # TODO: more efficient version with globs?
    }
