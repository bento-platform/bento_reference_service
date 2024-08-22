from bento_lib.drs.resolver import DrsResolver
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency

__all__ = [
    "get_drs_resolver",
    "DrsResolverDependency",
]


@lru_cache
def get_drs_resolver(config: ConfigDependency):
    return DrsResolver(cache_ttl=config.drs_cache_ttl)


DrsResolverDependency = Annotated[DrsResolver, Depends(get_drs_resolver)]
