import pathlib
from bento_reference_service.models import Genome

__all__ = [
    "DATA_DIR",
    "SARS_COV_2_GENOME_ID",
    "SARS_COV_2_FASTA_PATH",
    "SARS_COV_2_FAI_PATH",
    "SARS_COV_2_GFF3_GZ_PATH",
    "SARS_COV_2_GFF3_GZ_TBI_PATH",
    "TEST_GENOME_SARS_COV_2",
    "TEST_GENOME_SARS_COV_2_OBJ",
    "HG38_CHR1_F100K_GENOME_ID",
    "HG38_CHR1_F100K_FAI_PATH",
    "TEST_GENOME_HG38_CHR1_F100K",
    "TEST_GENOME_HG38_CHR1_F100K_OBJ",
    "AUTHORIZATION_HEADER",
    "TEST_DRS_ID",
    "TEST_DRS_REPLY_NO_ACCESS",
    "TEST_DRS_REPLY",
]

DATA_DIR = (pathlib.Path(__file__).parent / "data").absolute()

SARS_COV_2_GENOME_ID = "MN908947.3"
SARS_COV_2_ALIAS = {"alias": "NC_045512.2", "naming_authority": "refseq"}
SARS_COV_2_FAKE_ALIAS = {"alias": "sars-cov-2", "naming_authority": "me-myself-and-i"}
SARS_COV_2_FASTA_PATH = DATA_DIR / "sars_cov_2.fa"
SARS_COV_2_FAI_PATH = DATA_DIR / "sars_cov_2.fa.fai"
SARS_COV_2_GFF3_GZ_PATH = DATA_DIR / "sars_cov_2.gff3.gz"
SARS_COV_2_GFF3_GZ_TBI_PATH = DATA_DIR / "sars_cov_2.gff3.gz.tbi"

TEST_GENOME_SARS_COV_2 = {
    "id": SARS_COV_2_GENOME_ID,
    "aliases": [SARS_COV_2_ALIAS, SARS_COV_2_FAKE_ALIAS],
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
            "aliases": [SARS_COV_2_ALIAS, SARS_COV_2_FAKE_ALIAS],
            "md5": "105c82802b67521950854a851fc6eefd",
            "ga4gh": "SQ.SyGVJg_YRedxvsjpqNdUgyyqx7lUfu_D",
            "length": 29903,
            "circular": False,
        },
    ],
}
TEST_GENOME_SARS_COV_2_OBJ = Genome(**TEST_GENOME_SARS_COV_2)

HG38_CHR1_F100K_GENOME_ID = "hg38-chr1-f100k"
HG38_CHR1_F100K_FAI_PATH = DATA_DIR / "hg38.chr1.f100k.fa.fai"
TEST_GENOME_HG38_CHR1_F100K = {
    "id": HG38_CHR1_F100K_GENOME_ID,
    "md5": "80c4a2f1d70d2ca5babe40ca24e47e85",
    "ga4gh": "SQ.Sd58mcdOdfBAdpwaLFeI5bHwjspHd2D6",
    "fasta": f"file://{DATA_DIR / 'hg38.chr1.f100k.fa'}",
    "fai": f"file://{HG38_CHR1_F100K_FAI_PATH}",
    "gff3_gz": f"file://{DATA_DIR / 'gencode.v45.first-few.gff3.gz'}",
    "gff3_gz_tbi": f"file://{DATA_DIR / 'gencode.v45.first-few.gff3.gz.tbi'}",
    "taxon": {"id": "NCBITaxon:9606", "label": "Homo sapiens"},
    "contigs": [
        {
            "name": "chr1",
            "aliases": [],
            "md5": "d12b28d76aa3c1c6bb143b8da8cce642",
            "ga4gh": "SQ.jTVrjy4tzSYmexXZs_cfFWNuRKpvpVBI",
            "length": 100000,
            "circular": False,
        }
    ],
}
TEST_GENOME_HG38_CHR1_F100K_OBJ = Genome(**TEST_GENOME_HG38_CHR1_F100K)

AUTHORIZATION_HEADER = {"Authorization": "Token bearer"}


# Test DRS responses

TEST_DRS_ID = "dd11912c-3433-4a0a-8a01-3c0699288bef"

TEST_DRS_REPLY_NO_ACCESS = {
    "checksums": [
        {
            "checksum": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            "type": "sha-256",
        }
    ],
    "created_time": "2021-03-17T21:29:15+00:00",
    "updated_time": "2021-03-17T21:29:15+00:00",
    "id": TEST_DRS_ID,
    "mime_type": "text/plain",
    "self_uri": f"drs://localhost/{TEST_DRS_ID}",
    "size": 4,
}

TEST_DRS_REPLY = {
    **TEST_DRS_REPLY_NO_ACCESS,
    "access_methods": [
        {
            "type": "file",
            "access_url": {"url": "file:///test.txt"},
        },
        {
            "type": "https",
            "access_url": {"url": "https://example.org/test.txt"},
        },
    ],
}
