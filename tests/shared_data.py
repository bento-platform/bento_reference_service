import pathlib

__all__ = [
    "DATA_DIR",
    "SARS_COV_2_GENOME_ID",
    "TEST_GENOME_OF_FILE_URIS",
]

DATA_DIR = (pathlib.Path(__file__).parent / "data").absolute()

SARS_COV_2_GENOME_ID = "NC_045512.2"

TEST_GENOME_OF_FILE_URIS = {
    "id": SARS_COV_2_GENOME_ID,
    "aliases": [],
    "md5": "825ab3c54b7a67ff2db55262eb532438",
    "ga4gh": "SQ.mMg8qNej7pU84juQQWobw9JyUy09oYdd",
    "fasta": f"file://{DATA_DIR / 'sars_cov_2.fa'}",
    "fai": f"file://{DATA_DIR / 'sars_cov_2.fa.fai'}",
    "taxon": {"id": "NCBITaxon:2697049", "label": "Severe acute respiratory syndrome coronavirus 2"},
    "contigs": [
        {
            "name": SARS_COV_2_GENOME_ID,
            "aliases": [],
            "md5": "105c82802b67521950854a851fc6eefd",
            "ga4gh": "SQ.SyGVJg_YRedxvsjpqNdUgyyqx7lUfu_D",
            "length": 29903,
            "circular": False,
        },
    ],
}
