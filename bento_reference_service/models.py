from pydantic import BaseModel

from typing import List

# Pydantic models, not database models


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

    contigs: List[Contig]
