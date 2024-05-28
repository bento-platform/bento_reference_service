import pytest
from typing import Type

from bento_reference_service.fai import parse_fai

from .shared_data import SARS_COV_2_FAI_PATH, HG38_CHR1_F100K_FAI_PATH

INVALID_FAI_1 = b"chr1\tabc\t87\t60\t61\n"
INVALID_FAI_2 = b"chr1\t29903\t87\t60\t61\t42\n"


def test_valid_fai_parsing():
    with open(SARS_COV_2_FAI_PATH, "rb") as fh:
        assert parse_fai(fh.read()) == {"MN908947.3": (29903, 87, 60, 61)}

    with open(HG38_CHR1_F100K_FAI_PATH, "rb") as fh:
        assert parse_fai(fh.read()) == {"chr1": (100000, 6, 50, 51)}


@pytest.mark.parametrize("invalid_fai,exc", [(INVALID_FAI_1, ValueError), (INVALID_FAI_2, ValueError)])
def test_invalid_fai_parsing(invalid_fai: bytes, exc: Type[Exception]):
    with pytest.raises(exc):
        parse_fai(invalid_fai)
