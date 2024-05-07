from pydantic import BaseModel
from typing import Literal

__all__ = [
    "OntologyTerm",
    "Alias",
    "Contig",
    "ContigWithRefgetURI",
    "Genome",
    "GenomeWithURIs",
    "GenomeFeatureEntry",
    "GenomeFeature",
]

# Pydantic/dict models, not database models


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
    name: str
    aliases: tuple[Alias, ...] = ()

    # checksums for sequence (content-based addressing)
    md5: str
    ga4gh: str

    length: int  # Length of sequence
    circular: bool = False


class ContigWithRefgetURI(Contig):
    refget_uris: tuple[str, ...]


class Genome(BaseModel):
    id: str
    aliases: tuple[Alias, ...] = ()

    # checksums for FASTA
    md5: str
    ga4gh: str

    fasta: str  # URI
    fai: str  # URI

    gff3_gz: str | None = None  # URI
    gff3_gz_tbi: str | None = None  # URI

    # biological information
    taxon: OntologyTerm  # MUST be from NCBITaxon ontology - ingestion SHOULD validate this
    contigs: tuple[Contig, ...]


class GenomeWithURIs(Genome):
    uri: str
    contigs: tuple[ContigWithRefgetURI, ...]


class GenomeFeatureEntry(BaseModel):
    start_pos: int  # 1-based, inclusive
    end_pos: int  # 1-based, exclusive
    score: float | None
    phase: int | None


class GenomeFeature(BaseModel):
    genome_id: str
    contig_name: str

    strand: Literal["negative", "positive", "unknown", "not_stranded"]

    feature_id: str
    feature_name: str
    feature_type: str

    source: str

    entries: list[GenomeFeatureEntry]
    annotations: dict[str, list[str]]

    parents: tuple[str, ...]
