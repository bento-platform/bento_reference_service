import logging
import pysam
import re

from pathlib import Path
from typing import Generator
from urllib.parse import unquote as url_unquote

from . import models as m
from .db import Database

__all__ = [
    "ingest_gene_feature_annotation",
]


VALID_GENOME_ID = re.compile(r"^[a-zA-Z0-9\-_.]{3,50}$")


class AnnotationIngestError(Exception):
    pass


def parse_attributes(raw_attributes: dict[str, str]) -> dict[str, list[str]]:
    # See "attributes" in http://gmod.org/wiki/GFF3
    return {k: [url_unquote(e) for e in v.split(",")] for k, v in raw_attributes.items()}


def extract_feature_name(record, attributes: dict[str, list[str]]) -> str | None:
    feature_type = record.feature
    feature_name: str | None = attributes.get("Name", (None,))[0]

    if feature_name:
        return feature_name

    transcript_id = attributes.get("transcript_id", (None,))[0]
    transcript_name = attributes.get("transcript_name", (transcript_id,))[0]

    match feature_type:
        case "gene":
            return attributes.get("gene_name", attributes.get("gene_id", (None,)))[0]
        case "transcript":
            return attributes.get("transcript_name", attributes.get("transcript_id", (None,)))[0]
        case "5UTR" | "five_prime_utr":  # 5' untranslated region (UTR)
            return f"{transcript_name} 5' UTR"
        case "3UTR" | "three_prime_utr":  # 3' untranslated region (UTR)
            return f"{transcript_name} 3' UTR"
        case "start_codon":
            return f"{transcript_name} start codon"
        case "stop_codon":
            return f"{transcript_name} stop codon"
        case "exon":
            if "exon_id" in attributes:
                return attributes["exon_id"][0]
            else:
                return attributes["ID"][0]
        case "CDS":  # coding sequence
            return f"{transcript_name} CDS"
        case _:
            return None


async def ingest_gene_feature_annotation(
    genome_id: str,
    gff_path: Path,
    gff_index_path: Path,
    db: Database,
    logger: logging.Logger,
) -> None:
    """
    Given a genome ID and a path to an external GTF gene/exon/transcript annotation file, this function copies the GTF
    into the relevant .bentoGenome directory and ingests the annotations into an ElasticSearch index for fuzzy text
    querying of features.
    :param genome_id: The ID of the genome to attach the annotation to.
    :param gff_path: The path to an external GTF.gz-formatted annotation file to copy and read from.
    :param gff_index_path: The path to an external index file for the above .gtf.gz.
    :param db: Database connection/management object.
    :param logger: Python logger object.
    :return: None
    """

    # TODO: make sure it's a gtf.gz
    # TODO: copy it in place

    genome: m.GenomeWithURIs | None = await db.get_genome(genome_id)

    if genome is None:
        raise AnnotationIngestError(f"Genome with ID {genome_id} not found")

    log_progress_interval = 1000

    def _iter_features() -> Generator[m.GenomeFeature, None, None]:
        gff = pysam.TabixFile(str(gff_path), index=str(gff_index_path))
        total_processed: int = 0
        try:
            features_by_id: dict[str, m.GenomeFeature] = {}

            for contig in genome.contigs:
                logger.info(f"Indexing features from contig {contig.name}")
                features_by_id.clear()

                for record in gff.fetch(contig.name, parser=pysam.asGFF3()):
                    # for some reason, dict(...) returns the attributes dict:
                    record_attributes = parse_attributes(dict(record))

                    feature_type = record.feature
                    feature_id = record_attributes.get("ID", (None,))[0]
                    feature_name = extract_feature_name(record, record_attributes)

                    if feature_id is None:
                        logger.warning(f"Skipping unsupported feature (type={feature_type}, no ID retrieval): {record}")
                        continue

                    if feature_name is None:
                        logger.warning(f"Using ID as name for feature: {record}")
                        feature_name = feature_id

                    entry = m.GenomeFeatureEntry(
                        start_pos=record.start,
                        end_pos=record.end,
                        score=record.score,
                        phase=record.frame,  # misnamed in PySAM's GFF3 parser
                    )

                    if feature_id in features_by_id:
                        features_by_id[feature_id].entries.append(entry)
                    else:
                        features_by_id[feature_id] = m.GenomeFeature(
                            genome_id=genome_id,
                            contig_name=contig.name,
                            strand=record.strand,
                            feature_id=feature_id,
                            feature_name=feature_name,
                            feature_type=feature_type,
                            source=record.source,
                            entries=[entry],
                            attributes=record_attributes,
                            parents=tuple(p for p in record_attributes.get("Parent", "").split(",") if p),
                        )

                total_processed += 1
                if total_processed % log_progress_interval == 0:
                    logger.info(f"Processed {total_processed} features")

            yield from features_by_id.values()

        finally:
            gff.close()
