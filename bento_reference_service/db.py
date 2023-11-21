from bento_lib.db.pg_async import PgAsyncDatabase
from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from .config import ConfigDependency


SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"


class Database(PgAsyncDatabase):
    pass  # TODO


@lru_cache()
def get_db(config: ConfigDependency) -> Database:  # pragma: no cover
    return Database(config.database_uri, SCHEMA_PATH)


DatabaseDependency = Annotated[Database, Depends(get_db)]
