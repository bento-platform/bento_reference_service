import asyncio
import asyncpg
import pytest
import pytest_asyncio

from fastapi.testclient import TestClient
from typing import AsyncGenerator

import os

os.environ["BENTO_DEBUG"] = "true"
os.environ["CORS_ORIGINS"] = "*"
os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"

from bento_reference_service.config import get_config
from bento_reference_service.db import Database, get_db
from bento_reference_service.main import app


async def get_test_db() -> AsyncGenerator[Database, None]:
    db_instance = Database(get_config())
    await db_instance.initialize(pool_size=1)  # Small pool size for testing
    yield db_instance


db_fixture = pytest_asyncio.fixture(get_test_db, name="db")


@pytest_asyncio.fixture
async def db_cleanup(db: Database):
    yield
    conn: asyncpg.Connection
    async with db.connect() as conn:
        await conn.execute(
            """
        DROP INDEX genome_features_feature_name_trgm_gin;
        DROP INDEX genome_features_position_text_trgm_gin;
        DROP INDEX annotations_genome_feature_attr_idx;

        DROP TABLE genome_feature_annotations;
        DROP TABLE genome_feature_parents;
        DROP TABLE genome_features;
        
        DROP TABLE genome_feature_type_synonyms;
        DROP TABLE genome_feature_types;
        
        DROP TABLE genome_contig_aliases;
        DROP TABLE genome_contigs;
        
        DROP TABLE genome_aliases;
        DROP TABLE genomes;

        DROP TYPE strand_type;
        """
        )
    await db.close()


# noinspection PyUnusedLocal
@pytest.fixture
def test_client(db: Database):
    with TestClient(app) as client:
        app.dependency_overrides[get_db] = get_test_db
        yield client


# noinspection PyUnusedLocal
@pytest.fixture(scope="session")
def event_loop(request):
    # Create an instance of the default event loop for each test case.
    # See https://github.com/pytest-dev/pytest-asyncio/issues/38#issuecomment-264418154
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
