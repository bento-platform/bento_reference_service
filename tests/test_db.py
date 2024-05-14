from bento_reference_service.db import Database

from .shared_data import TEST_GENOME_OF_FILE_URIS


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
