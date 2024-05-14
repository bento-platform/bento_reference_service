from bento_reference_service.db import Database

from .shared_data import TEST_GENOME_OF_FILE_URIS, TEST_GENOME_HG38_CHR1_F100K


async def test_create_genome(db: Database, db_cleanup):
    # SARS-CoV-2
    await db.create_genome(TEST_GENOME_OF_FILE_URIS, return_external_resource_uris=False)

    # hg38 chr1:1-100000
    await db.create_genome(TEST_GENOME_HG38_CHR1_F100K, return_external_resource_uris=False)


async def test_mark_running_as_error(db: Database, db_cleanup):
    g = await db.create_genome(TEST_GENOME_OF_FILE_URIS, return_external_resource_uris=False)

    t1 = await db.create_task(g.id, "ingest_features")
    t2 = await db.create_task(g.id, "ingest_features")
    await db.update_task_status(t2, "running")

    assert (await db.get_task(t1)).status == "queued"
    assert (await db.get_task(t2)).status == "running"

    await db.move_running_tasks_to_error()

    assert (await db.get_task(t1)).status == "error"
    assert (await db.get_task(t2)).status == "error"
