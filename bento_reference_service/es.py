from elasticsearch import AsyncElasticsearch
from .config import config
from .indices import ALL_INDICES

__all__ = [
    "es",
    "create_all_indices",
]

es = AsyncElasticsearch(config.elasticsearch_url)


async def create_all_indices():
    for index in ALL_INDICES:
        if not await es.indices.exists(index=index["name"]):
            await es.indices.create(index=index["name"], mappings=index["mappings"])
