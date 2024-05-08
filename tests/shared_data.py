import pathlib

__all__ = [
    "DATA_DIR",
    "SARS_COV_2_GENOME_ID",
    "SARS_COV_2_FASTA_PATH",
    "SARS_COV_2_FAI_PATH",
    "SARS_COV_2_GFF3_GZ_PATH",
    "SARS_COV_2_GFF3_GZ_TBI_PATH",
    "TEST_GENOME_OF_FILE_URIS",
]

DATA_DIR = (pathlib.Path(__file__).parent / "data").absolute()

SARS_COV_2_GENOME_ID = "MN908947.3"
SARS_COV_2_FASTA_PATH = DATA_DIR / "sars_cov_2.fa"
SARS_COV_2_FAI_PATH = DATA_DIR / "sars_cov_2.fa.fai"
SARS_COV_2_GFF3_GZ_PATH = DATA_DIR / "sars_cov_2.gff3.gz"
SARS_COV_2_GFF3_GZ_TBI_PATH = DATA_DIR / "sars_cov_2.gff3.gz.tbi"

TEST_GENOME_OF_FILE_URIS = {
    "id": SARS_COV_2_GENOME_ID,
    "aliases": [],
    "md5": "b98334cd0015ee1b1d2dc3b9d81b325e",
    "ga4gh": "SQ.F4O8uhlkMQ76rmE6SmUFFjp04UV25Ybn",
    "fasta": f"file://{SARS_COV_2_FASTA_PATH}",
    "fai": f"file://{SARS_COV_2_FAI_PATH}",
    "gff3_gz": f"file://{SARS_COV_2_GFF3_GZ_PATH}",
    "gff3_gz_tbi": f"file://{SARS_COV_2_GFF3_GZ_TBI_PATH}",
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
