import aiofiles
import uuid

from .config import config
from .routers.data_types import DATA_TYPE_GENOME
from .schemas import GENOME_METADATA_SCHEMA


__all__ = [
    "table_id_path",
    "create_table_id_if_needed",
    "get_table_id",
]


table_id_path = config.data_path / "instance_table_id"


async def create_table_id_if_needed() -> None:
    async with aiofiles.open(table_id_path, "w") as tf:
        await tf.write(str(uuid.uuid4()))


async def get_table_id() -> str:
    await create_table_id_if_needed()
    async with aiofiles.open(table_id_path, "r") as tf:
        return (await tf.read()).strip()


async def get_table() -> dict:
    return {
        "id": await get_table_id(),
        "name": "Reference Genomes (instance-wide table)",
        "metadata": {},
        "data_type": DATA_TYPE_GENOME,
        "schema": GENOME_METADATA_SCHEMA,
    }
