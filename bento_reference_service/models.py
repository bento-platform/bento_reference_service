from pathlib import Path
from pydantic import BaseModel

from typing import List, TypedDict

__all__ = [
    "TDAlias",
    "Alias",
    "Contig",
    "Genome",
]

# Pydantic/dict models, not database models


class TDAlias(TypedDict):
    alias: str
    naming_authority: str


class Alias(BaseModel):
    """
    Alias for a genome or contig; modeled after alias representation from RefGet.
    """

    alias: str
    naming_authority: str


class Contig(BaseModel):
    name: str
    aliases: List[Alias]

    # checksums for sequence (content-based addressing)
    md5: str
    trunc512: str


class Genome(BaseModel):
    id: str
    aliases: List[Alias]

    # checksums for FASTA
    md5: str
    trunc512: str

    fasta: Path
    fai: Path

    contigs: List[Contig]
