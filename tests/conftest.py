import asyncpg
import logging
import os
import pytest
import pytest_asyncio
import structlog.stdlib
import structlog.testing

from aioresponses import aioresponses
from bento_lib.drs.resolver import DrsResolver
from fastapi.testclient import TestClient
from typing import AsyncGenerator

os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_VALIDATE_SSL"] = "false"
os.environ["BENTO_JSON_LOGS"] = "false"  # use rich text logs in testing
os.environ["CORS_ORIGINS"] = "*"
os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"

from bento_reference_service.config import Config, get_config
from bento_reference_service.db import Database, get_db
from bento_reference_service.drs import get_drs_resolver
from bento_reference_service.logger import get_logger
from bento_reference_service.main import app

from .shared_data import TEST_GENOME_SARS_COV_2
from .shared_functions import create_genome_with_permissions


@pytest.fixture(name="log_output")
def fixture_log_output():
    # INFO: see https://www.structlog.org/en/stable/testing.html
    return structlog.testing.LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output):
    logging.getLogger("asyncio").setLevel(logging.WARN)
    logging.getLogger("httpx").setLevel(logging.WARN)

    # INFO: see https://www.structlog.org/en/stable/testing.html
    structlog.configure(processors=[log_output])


@pytest.fixture()
def config() -> Config:
    return get_config()


@pytest.fixture()
def drs_resolver(config: Config) -> DrsResolver:
    drs = get_drs_resolver(config)
    drs._cache_ttl = 0
    drs._drs_record_cache = {}
    return drs


async def get_test_db() -> AsyncGenerator[Database, None]:
    config = get_config()
    db_instance = Database(config, get_logger(config))
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
            DROP TABLE IF EXISTS tasks;
            DROP TYPE IF EXISTS task_kind;
            DROP TYPE IF EXISTS task_status;
            
            DROP VIEW genome_feature_attributes_view;
            
            DROP INDEX IF EXISTS genome_features_feature_id_trgm_gin;
            DROP INDEX IF EXISTS genome_features_feature_name_trgm_gin;
            DROP INDEX IF EXISTS genome_feature_entries_position_text_trgm_gin;
            DROP INDEX IF EXISTS genome_feature_attributes_attr_idx;
            DROP INDEX IF EXISTS genome_feature_attribute_keys_attr_idx;
            DROP INDEX IF EXISTS genome_feature_parents_feature_idx;
            DROP INDEX IF EXISTS genome_feature_parents_parent_idx;
            
            DROP TABLE IF EXISTS genome_feature_entries;
            DROP TABLE IF EXISTS genome_feature_attributes;
            DROP TABLE IF EXISTS genome_feature_attribute_keys;
            DROP TABLE IF EXISTS genome_feature_attribute_values;
            DROP TABLE IF EXISTS genome_feature_parents;
            DROP TABLE IF EXISTS genome_features;
            
            DROP TABLE IF EXISTS genome_feature_type_synonyms;
            DROP TABLE IF EXISTS genome_feature_types;
            
            DROP TABLE IF EXISTS genome_contig_aliases;
            DROP TABLE IF EXISTS genome_contigs;
            
            DROP TABLE IF EXISTS genome_aliases;
            DROP TABLE IF EXISTS genomes;
    
            DROP TYPE IF EXISTS strand_type;
            """
        )
    await db.close()


# noinspection PyUnusedLocal
@pytest.fixture
def test_client(db: Database):
    with TestClient(app) as client:
        app.dependency_overrides[get_db] = get_test_db
        yield client


@pytest.fixture
def aioresponse():
    with aioresponses() as m:
        yield m


@pytest.fixture
def sars_cov_2_genome(test_client: TestClient, aioresponse: aioresponses, db_cleanup):
    create_genome_with_permissions(test_client, aioresponse, TEST_GENOME_SARS_COV_2)
    return TEST_GENOME_SARS_COV_2
