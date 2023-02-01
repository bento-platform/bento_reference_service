from pathlib import Path
from pydantic import BaseModel

from typing import List, TypedDict

__all__ = [
    "GTFFeature",
    "Alias",
    "Contig",
    "Genome",
]

# Pydantic/dict models, not database models


class GTFFeature(TypedDict):
    id: str
    name: str
    position: str
    type: str
    genome: str
    strand: str


class OntologyTerm(BaseModel):
    id: str
    label: str


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

    # biological information
    taxon: OntologyTerm  # MUST be from NCBITaxon ontology - ingestion SHOULD validate this
    contigs: List[Contig]

