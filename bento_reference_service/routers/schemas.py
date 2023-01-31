from fastapi import APIRouter, HTTPException

from bento_reference_service.schemas import SCHEMAS_BY_FILE_NAME

__all__ = [
    "schema_router",
]


schema_router = APIRouter(prefix="/schemas")


@schema_router.get("/{schema_file}")
def get_schema_file(schema_file: str):
    if schema := SCHEMAS_BY_FILE_NAME.get(schema_file):
        return schema
    raise HTTPException(status_code=404, detail=f"schema file with name {schema_file} not found")
