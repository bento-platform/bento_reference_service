from pathlib import Path
from pydantic import BaseModel

from typing import List

__all__ = [
    "Alias",
    "Contig",
    "Genome",
]

# Pydantic/dict models, not database models


class Alias(BaseModel):
    """
    Alias for a genome or contig; modeled after alias representation from RefGet.
    """

    alias: str
    naming_authority: str


class Contig(BaseModel):
    genome: str

    name: str
    aliases: List[Alias]

    # checksums for sequence (content-based addressing)
    md5: str
    trunc512: str

    length: int  # Length of sequence
    circular: bool


class Genome(BaseModel):
    id: str
    aliases: List[Alias]
    uri: str

    # checksums for FASTA
    md5: str
    trunc512: str

    fasta: Path
    fai: Path

    contigs: List[Contig]
