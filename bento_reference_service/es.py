from elasticsearch import AsyncElasticsearch
from .config import config

__all__ = [
    "es",
]

es = AsyncElasticsearch(config.elasticsearch_url)
