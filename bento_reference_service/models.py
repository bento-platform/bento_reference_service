from datetime import datetime
from pydantic import BaseModel
from typing import Literal

__all__ = [
    "OntologyTerm",
    "Alias",
    "Contig",
    "ContigWithRefgetURI",
    "Genome",
    "GenomeWithURIs",
    "GenomeGFF3Patch",
    "GenomeFeatureEntry",
    "GenomeFeature",
    "TaskStatus",
    "TaskParams",
    "Task",
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


class GenomeGFF3Patch(BaseModel):
    gff3_gz: str  # URI
    gff3_gz_tbi: str  # URI


class GenomeFeatureEntry(BaseModel):
    start_pos: int  # 1-based, inclusive
    end_pos: int  # 1-based, exclusive
    score: float | None
    phase: int | None


class GenomeFeature(BaseModel):
    genome_id: str
    contig_name: str

    strand: Literal["-", "+", "?", "."]

    feature_id: str
    feature_name: str
    feature_type: str

    source: str

    entries: list[GenomeFeatureEntry]  # mutable to allow us to gradually build up entry list during ingestion

    gene_id: str | None  # extracted from attributes, since for GENCODE GFF3s this is a standardized and useful field
    attributes: dict[str, list[str]]

    parents: tuple[str, ...]


TaskStatus = Literal["queued", "running", "success", "error"]


class TaskParams(BaseModel):
    genome_id: str
    kind: Literal["ingest_features"]


class Task(TaskParams):
    id: int
    status: TaskStatus
    message: str
    created: datetime
