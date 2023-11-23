import logging
import pysam
import re

from elasticsearch.helpers import async_streaming_bulk
from pathlib import Path
from typing import Generator

from . import models as m
from .db import Database

__all__ = [
    "ingest_gene_feature_annotation",
]


VALID_GENOME_ID = re.compile(r"^[a-zA-Z0-9\-_.]{3,50}$")


class AnnotationIngestError(Exception):
    pass


def extract_feature_id_and_name(record) -> tuple[str | None, str | None]:
    feature_type = record.feature
    gene_id = record.gene_id
    gene_name = record.attributes.get("gene_name", gene_id)

    feature_id: str | None = None
    feature_name: str | None = None

    match feature_type:
        case "gene":
            feature_id = gene_id
            feature_name = gene_name
        case "transcript":
            feature_id = record.transcript_id
            feature_name = feature_id  # Explicitly re-use ID as name here
        case "5UTR" | "five_prime_utr":  # 5' untranslated region (UTR)
            feature_id = f"{gene_id}-5UTR"
            feature_name = f"{gene_name} 5' UTR"
        case "3UTR" | "three_prime_utr":  # 3' untranslated region (UTR)
            feature_id = f"{gene_id}-3UTR"
            feature_name = f"{gene_name} 3' UTR"
        case "start_codon":  # TODO: multiple start codons may exist?
            feature_id = f"{gene_id}-start_codon"
            feature_name = f"{gene_name} start codon"
        case "stop_codon":  # TODO: multiple stop codons may exist?
            feature_id = f"{gene_id}-stop_codon"
            feature_name = f"{gene_name} stop codon"
        case "exon":
            feature_id = record.attributes["exon_id"]  # TODO: fallback with gene ID + exon number?
            if "exon_number" in record.attributes:
                # TODO: Validate this, I think slightly wrong because it uses gene vs. transcript
                feature_name = f"{gene_name} exon {record.attributes['exon_number']}"
            else:
                feature_name = feature_id  # Explicitly re-use ID as name here
        case "CDS":  # coding sequence
            exon_id = record.attributes["exon_id"]
            feature_id = f"{exon_id}-CDS"
            if "exon_number" in record.attributes:
                # TODO: Validate this, I think slightly wrong because it uses gene vs. transcript
                feature_name = f"{gene_name} exon {record.attributes['exon_number']} CDS"
            else:
                feature_name = f"{exon_id} CDS"  # Explicitly re-use ID as name here

    return feature_id, feature_name


async def ingest_gene_feature_annotation(
    genome_id: str,
    gtf_annotation_path: Path,
    gtf_annotation_index_path: Path,
    db: Database,
    logger: logging.Logger,
) -> None:
    """
    Given a genome ID and a path to an external GTF gene/exon/transcript annotation file, this function copies the GTF
    into the relevant .bentoGenome directory and ingests the annotations into an ElasticSearch index for fuzzy text
    querying of features.
    :param genome_id: The ID of the genome to attach the annotation to.
    :param gtf_annotation_path: The path to an external GTF.gz-formatted annotation file to copy and read from.
    :param gtf_annotation_index_path: The path to an external index file for the above .gtf.gz.
    :param db: Database connection/management object.
    :param logger: Python logger object.
    :return: None
    """

    # TODO: make sure it's a gtf.gz
    # TODO: copy it in place

    genome: m.GenomeWithURIs | None = await db.get_genome(genome_id)

    log_progress_interval = 1000

    def _iter_features() -> Generator[m.GTFFeature, None, None]:
        gtf = pysam.TabixFile(str(gtf_annotation_path), index=str(gtf_annotation_index_path))
        total_processed: int = 0
        try:
            for contig in genome.contigs:
                logger.info(f"Indexing features from contig {contig.name}")

                for record in gtf.fetch(contig.name, parser=pysam.asGTF()):
                    feature_type = record.feature
                    feature_id, feature_name = extract_feature_id_and_name(record)

                    if feature_id is None:
                        logger.warning(f"Skipping unsupported feature (type={feature_type}, no ID retrieval): {record}")
                        continue

                    if feature_name is None:
                        logger.warning(f"Using ID as name for feature: {record}")
                        feature_name = feature_id

                    yield {
                        "id": feature_id,
                        "name": feature_name,
                        "position": f"{contig.name}:{record.start}-{record.end}",
                        "type": feature_type,
                        "genome": genome_id,
                        "strand": record.strand,
                    }

                total_processed += 1
                if total_processed % log_progress_interval == 0:
                    logger.info(f"Processed {total_processed} features")

        finally:
            gtf.close()

    async for ok, result in async_streaming_bulk(es, _iter_features()):
        if not ok:
            action, result = result.popitem()
            raise AnnotationIngestError(f"failed to {action} document: {result}")
