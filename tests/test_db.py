import logging

import pytest

from pathlib import Path

from bento_reference_service.db import Database
from bento_reference_service.features import ingest_features

from .shared_data import SARS_COV_2_GENOME_ID, TEST_GENOME_SARS_COV_2_OBJ, TEST_GENOME_HG38_CHR1_F100K_OBJ

pytestmark = pytest.mark.asyncio()


async def _set_up_sars_cov_2_genome(db: Database):
    await db.create_genome(TEST_GENOME_SARS_COV_2_OBJ, return_external_resource_uris=False)


async def _set_up_hg38_subset_genome(db: Database):
    await db.create_genome(TEST_GENOME_HG38_CHR1_F100K_OBJ, return_external_resource_uris=False)


async def test_create_genome(db: Database, db_cleanup):
    await _set_up_sars_cov_2_genome(db)
    await _set_up_hg38_subset_genome(db)


@pytest.mark.parametrize(
    "checksum,genome_id,contig_name",
    [
        ("SQ.SyGVJg_YRedxvsjpqNdUgyyqx7lUfu_D", SARS_COV_2_GENOME_ID, "MN908947.3"),
        ("ga4gh:SQ.SyGVJg_YRedxvsjpqNdUgyyqx7lUfu_D", SARS_COV_2_GENOME_ID, "MN908947.3"),
        ("105c82802b67521950854a851fc6eefd", SARS_COV_2_GENOME_ID, "MN908947.3"),
        ("md5:105c82802b67521950854a851fc6eefd", SARS_COV_2_GENOME_ID, "MN908947.3"),
        ("d12b28d76aa3c1c6bb143b8da8cce642", TEST_GENOME_HG38_CHR1_F100K_OBJ.id, "chr1:1-100000"),
        ("md5:d12b28d76aa3c1c6bb143b8da8cce642", TEST_GENOME_HG38_CHR1_F100K_OBJ.id, "chr1:1-100000"),
    ],
)
async def test_get_genome_and_contig_by_checksum_str(db: Database, db_cleanup, checksum, genome_id, contig_name):
    # start with two genomes, so we validate that we get the right one
    await _set_up_sars_cov_2_genome(db)
    await _set_up_hg38_subset_genome(db)

    res = await db.get_genome_and_contig_by_checksum_str(checksum)
    assert res is not None
    g_res, c_res = res
    assert g_res.id == genome_id
    assert c_res.name == contig_name


async def test_get_genome_and_contig_by_checksum_str_dne(db: Database, db_cleanup):
    await _set_up_sars_cov_2_genome(db)
    res = await db.get_genome_and_contig_by_checksum_str("DOES_NOT_EXIST")
    assert res is None


async def test_mark_running_as_error(db: Database, db_cleanup):
    await _set_up_sars_cov_2_genome(db)

    t1 = await db.create_task(SARS_COV_2_GENOME_ID, "ingest_features")
    t2 = await db.create_task(SARS_COV_2_GENOME_ID, "ingest_features")
    await db.update_task_status(t2, "running")

    assert (await db.get_task(t1)).status == "queued"
    assert (await db.get_task(t2)).status == "running"

    await db.move_running_tasks_to_error()

    assert (await db.get_task(t1)).status == "error"
    assert (await db.get_task(t2)).status == "error"


# TODO: fixture
async def _set_up_sars_cov_2_genome_and_features(db: Database, logger: logging.Logger):
    await _set_up_sars_cov_2_genome(db)

    # prerequesite: ingest features
    gff3_gz_path = Path(TEST_GENOME_SARS_COV_2_OBJ.gff3_gz.replace("file://", ""))
    gff3_gz_tbi_path = Path(TEST_GENOME_SARS_COV_2_OBJ.gff3_gz_tbi.replace("file://", ""))
    await ingest_features(SARS_COV_2_GENOME_ID, gff3_gz_path, gff3_gz_tbi_path, db, logger)


async def test_genome_features_summary(db: Database, db_cleanup):
    logger = logging.getLogger(__name__)
    await _set_up_sars_cov_2_genome_and_features(db, logger)
    s = await db.genome_feature_types_summary(SARS_COV_2_GENOME_ID)
    assert sum(s.values()) == 49  # total # of features, divided by type in summary response


async def test_filter_and_query_genome_features(db: Database, db_cleanup):
    logger = logging.getLogger(__name__)
    await _set_up_sars_cov_2_genome_and_features(db, logger)

    # - should get back 2 genes and 1 transcript
    res, page = await db.filter_genome_features(SARS_COV_2_GENOME_ID, name="ORF1ab")
    assert len(res) == 3
    assert page["total"] == 3


async def test_query_genome_features(db: Database, db_cleanup):
    logger = logging.getLogger(__name__)
    await _set_up_sars_cov_2_genome_and_features(db, logger)

    # - should get back 2 genes and 1 transcript
    res, page = await db.query_genome_features(SARS_COV_2_GENOME_ID, q="ORF1ab")
    assert len(res) == 3
    assert page["total"] == 3

    # - filter by q - should get back 1 gene
    res, page = await db.query_genome_features(SARS_COV_2_GENOME_ID, q="ENSSASG00005000002")
    assert len(res) == 1
    assert page["total"] == 1
    assert res[0].feature_id == "gene:ENSSASG00005000002"

    # - query by attribute value
    res, page = await db.query_genome_features(SARS_COV_2_GENOME_ID, q="protein_coding", limit=100)  # protein_coding
    assert len(res) == 24
    assert page["total"] == 24
