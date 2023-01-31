from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from ..schemas import GENOME_METADATA_SCHEMA


__all__ = [
    "DATA_TYPE_GENOME",
    "DATA_TYPES",
    "data_type_router",
]


DATA_TYPE_GENOME = "genome"

DATA_TYPES = {
    DATA_TYPE_GENOME: {
        "label": "Reference Genome",
        "schema": GENOME_METADATA_SCHEMA,
        "metadata_schema": {
            "type": "object",  # TODO
        },
    }
}


data_type_router = APIRouter(prefix="/data-types")


@data_type_router.get("")
def data_type_list():
    return JSONResponse([dt for dt in DATA_TYPES.values()])


@data_type_router.get("/{data_type}")
def data_type_detail(data_type: str):
    if dt := DATA_TYPES.get(data_type):
        return dt

    raise HTTPException(status_code=404, detail=f"data type not found: {data_type}")


@data_type_router.get("/{data_type}/schema")
def data_type_schema(data_type: str):
    if dt := DATA_TYPES.get(data_type):
        return dt["schema"]

    raise HTTPException(status_code=404, detail=f"data type not found: {data_type}")


@data_type_router.get("/{data_type}/metadata_schema")
def data_type_metadata_schema(data_type: str):
    if dt := DATA_TYPES.get(data_type):
        return dt["metadata_schema"]

    raise HTTPException(status_code=404, detail=f"data type not found: {data_type}")
