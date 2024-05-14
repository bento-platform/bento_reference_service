import logging

import pytest

from pathlib import Path

from bento_reference_service.db import Database
from bento_reference_service.features import ingest_features

from .shared_data import TEST_GENOME_SARS_COV_2_OBJ, TEST_GENOME_HG38_CHR1_F100K_OBJ

pytestmark = pytest.mark.asyncio()


async def test_create_genome(db: Database, db_cleanup):
    # SARS-CoV-2
    await db.create_genome(TEST_GENOME_SARS_COV_2_OBJ, return_external_resource_uris=False)

    # hg38 chr1:1-100000
    await db.create_genome(TEST_GENOME_HG38_CHR1_F100K_OBJ, return_external_resource_uris=False)


async def test_mark_running_as_error(db: Database, db_cleanup):
    g = await db.create_genome(TEST_GENOME_SARS_COV_2_OBJ, return_external_resource_uris=False)

    t1 = await db.create_task(g.id, "ingest_features")
    t2 = await db.create_task(g.id, "ingest_features")
    await db.update_task_status(t2, "running")

    assert (await db.get_task(t1)).status == "queued"
    assert (await db.get_task(t2)).status == "running"

    await db.move_running_tasks_to_error()

    assert (await db.get_task(t1)).status == "error"
    assert (await db.get_task(t2)).status == "error"


async def test_query_genome_features(db: Database, db_cleanup):
    logger = logging.getLogger(__name__)

    # prerequesite: create genome
    await db.create_genome(TEST_GENOME_SARS_COV_2_OBJ, return_external_resource_uris=False)

    # prerequesite: ingest features
    gff3_gz_path = Path(TEST_GENOME_SARS_COV_2_OBJ.gff3_gz.replace("file://", ""))
    gff3_gz_tbi_path = Path(TEST_GENOME_SARS_COV_2_OBJ.gff3_gz_tbi.replace("file://", ""))
    await ingest_features(TEST_GENOME_SARS_COV_2_OBJ.id, gff3_gz_path, gff3_gz_tbi_path, db, logger)

    g_id = TEST_GENOME_SARS_COV_2_OBJ.id

    # - should get back 2 genes and 1 transcript
    res, page = await db.query_genome_features(g_id, q="ORF1ab")
    assert len(res) == 3
    assert page["total"] == 3

    # - should get back 2 genes and 1 transcript
    res, page = await db.query_genome_features(g_id, name="ORF1ab")
    assert len(res) == 3
    assert page["total"] == 3

    # - filter by q and name - should get back 1 gene
    res, page = await db.query_genome_features(g_id, q="ENSSASG00005000002", name="ORF1ab")
    assert len(res) == 1
    assert page["total"] == 1
    assert res[0].feature_id == "gene:ENSSASG00005000002"
