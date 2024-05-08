import itertools
import logging
import pysam
import traceback

from pathlib import Path
from typing import Generator
from urllib.parse import unquote as url_unquote

from . import models as m
from .db import Database

__all__ = [
    "ingest_gene_feature_annotation",
]


GFF_CAPTURED_ATTRIBUTES = frozenset({"ID", "Parent"})
GFF_SKIPPED_FEATURE_TYPES = frozenset({"stop_codon_redefined_as_selenocysteine"})
GFF_BATCH_SIZE = 5000
GFF_LOG_PROGRESS_INTERVAL = 1000


class AnnotationIngestError(Exception):
    pass


def parse_attributes(raw_attributes: dict[str, str]) -> dict[str, list[str]]:
    # See "attributes" in http://gmod.org/wiki/GFF3
    return {k: [url_unquote(e) for e in str(v).split(",") if e] for k, v in raw_attributes.items()}


def extract_feature_id(record, attributes: dict[str, list[str]]) -> str | None:
    feature_type = record.feature.lower()
    feature_id = attributes.get("ID", (None,))[0]

    if feature_id:
        return feature_id

    match feature_type:
        case "gene":
            return attributes.get("gene_id", (None,))[0]
        case "transcript":
            return attributes.get("transcript_id", (None,))[0]
        case "exon":
            return attributes.get("exon_id", (None,))[0]
        case _:  # no alternative ID attribute to use, so we couldn't figure anything out.
            return None


def extract_feature_name(record, attributes: dict[str, list[str]]) -> str | None:
    feature_type = record.feature.lower()
    feature_name: str | None = attributes.get("Name", (None,))[0]

    if feature_name:
        return feature_name

    transcript_name = attributes.get("transcript_name", attributes.get("transcript_id", (None,)))[0]

    match feature_type:
        case "gene":
            return attributes.get("gene_name", attributes.get("gene_id", (None,)))[0]
        case "transcript":
            return transcript_name
        case "5utr" | "five_prime_utr":  # 5' untranslated region (UTR)
            return f"{transcript_name} 5' UTR"
        case "3utr" | "three_prime_utr":  # 3' untranslated region (UTR)
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
        case "cds":  # coding sequence
            return f"{transcript_name} CDS"
        case _:
            return None


async def ingest_gene_feature_annotation(
    # parameters:
    genome_id: str,
    gff_path: Path,
    gff_index_path: Path,
    # dependencies:
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

    genome: m.GenomeWithURIs | None = await db.get_genome(genome_id)

    if genome is None:
        raise AnnotationIngestError(f"Genome with ID {genome_id} not found")

    def _iter_features() -> Generator[m.GenomeFeature, None, None]:
        gff = pysam.TabixFile(str(gff_path), index=str(gff_index_path))
        total_processed: int = 0

        try:
            features_by_id: dict[str, m.GenomeFeature] = {}

            for contig in genome.contigs:
                logger.info(f"Indexing features from contig {contig.name}")

                try:
                    fetch_iter = gff.fetch(contig.name, parser=pysam.asGFF3())
                except ValueError as e:
                    logger.warning(f"Could not find contig with name {contig.name} in GFF3; skipping... ({e})")
                    continue

                for i, record in enumerate(fetch_iter):
                    feature_type = record.feature

                    if feature_type in GFF_SKIPPED_FEATURE_TYPES:
                        continue  # Don't ingest stop_codon_redefined_as_selenocysteine annotations

                    # for some reason, dict(...) returns the attributes dict:
                    feature_raw_attributes = dict(record)

                    try:
                        record_attributes = parse_attributes(feature_raw_attributes)
                        feature_id = extract_feature_id(record, record_attributes)
                        feature_name = extract_feature_name(record, record_attributes)

                        if feature_id is None:
                            logger.warning(
                                f"Skipping unsupported feature {i}: type={feature_type}, no ID retrieval; {record}"
                            )
                            continue

                        if feature_name is None:
                            logger.warning(f"Using ID as name for feature {i}: {record}")
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
                                strand=record.strand or ".",  # None/"." <=> unstranded
                                feature_id=feature_id,
                                feature_name=feature_name,
                                feature_type=feature_type,
                                source=record.source,
                                entries=[entry],
                                attributes={
                                    # skip attributes which have been captured in the above information
                                    k: v
                                    for k, v in record_attributes.items()
                                    if k not in GFF_CAPTURED_ATTRIBUTES
                                },
                                parents=tuple(p for p in record_attributes.get("Parent", ()) if p),
                            )

                    except Exception as e:
                        logger.error(
                            f"Could not process feature {i}: {feature_type=}, {feature_raw_attributes=}; encountered "
                            f"exception: {e}"
                        )
                        logger.error(traceback.format_exc())

                    total_processed += 1
                    if total_processed % GFF_LOG_PROGRESS_INTERVAL == 0:
                        logger.info(f"Processed {total_processed} features")

                yield from features_by_id.values()
                features_by_id.clear()

        finally:
            gff.close()

    features_to_ingest = _iter_features()

    n_ingested: int = 0

    while data := tuple(itertools.islice(features_to_ingest, GFF_BATCH_SIZE)):  # take features in batches
        await db.bulk_ingest_genome_features(data)
        n_ingested += len(data)

    if n_ingested == 0:
        raise AnnotationIngestError("No gene features could be ingested - is this a valid GFF3 file?")

    logger.info(f"Ingested {n_ingested} gene features")
