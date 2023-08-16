from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import Config, ConfigDependency
from .indices import make_genome_index_def, make_gene_feature_index_def

__all__ = [
    "get_es",
    "ESDependency",
]


@lru_cache()
async def get_es(config: ConfigDependency) -> AsyncElasticsearch:
    es = AsyncElasticsearch(config.elasticsearch_url)
    await create_all_indices(config, es)
    try:
        yield es
    finally:
        await es.close()


ESDependency = Annotated[AsyncElasticsearch, Depends(get_es)]


async def create_all_indices(config: Config, es: AsyncElasticsearch) -> None:
    all_indices = (
        make_genome_index_def(config),
        make_gene_feature_index_def(config),
    )

    for index in all_indices:
        if not await es.indices.exists(index=index["name"]):
            await es.indices.create(index=index["name"], mappings=index["mappings"])
