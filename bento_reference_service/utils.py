from fastapi import HTTPException

from . import models
from .config import Config
from .genomes import make_genome_path, get_genome

__all__ = [
    "make_uri",
    "get_genome_or_error",
]


def make_uri(path: str, config: Config) -> str:
    return f"{config.service_url_base_path.rstrip('/')}{path}"


async def get_genome_or_error(genome_id: str, config: Config) -> models.Genome:
    genome_path = make_genome_path(genome_id, config)

    if not genome_path.exists():
        raise HTTPException(status_code=404, detail=f"genome not found: {genome_id}")

    # TODO: handle format errors with 500
    return await get_genome(genome_path, config)
