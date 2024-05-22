import aiofiles
import logging
import pysam
import traceback

from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import unquote as url_unquote
from uuid import uuid4

from . import models as m
from .config import Config
from .db import Database
from .streaming import stream_from_uri

__all__ = [
    "INGEST_FEATURES_TASK_KIND",
    "AnnotationGenomeNotFoundError",
    "ingest_features",
    "ingest_features_task",
]

INGEST_FEATURES_TASK_KIND = "ingest_features"

GFF_ID_ATTR = "ID"
GFF_NAME_ATTR = "Name"
GFF_PARENT_ATTR = "Parent"
GFF_GENCODE_GENE_ID_ATTR = "gene_id"
GFF_CAPTURED_ATTRIBUTES = frozenset({GFF_ID_ATTR, GFF_NAME_ATTR, GFF_PARENT_ATTR, GFF_GENCODE_GENE_ID_ATTR})
GFF_SKIPPED_FEATURE_TYPES = frozenset({"stop_codon_redefined_as_selenocysteine"})
GFF_LOG_PROGRESS_INTERVAL = 100000


class AnnotationGenomeNotFoundError(Exception):
    pass


class AnnotationIngestError(Exception):
    pass


def parse_attributes(raw_attributes: dict[str, str]) -> dict[str, list[str]]:
    """
    Parse the raw GFF3 attribute dictionary into a properly list-ified dictionary - every attribute in GFF3 except a few
    standard ones can be lists (although most are not, in reality.)
    """

    # See "attributes" in http://gmod.org/wiki/GFF3
    return {k: [url_unquote(e) for e in str(v).split(",") if e] for k, v in raw_attributes.items()}


def extract_feature_id(record, attributes: dict[str, list[str]]) -> str | None:
    """
    Given a GFF3 record and an extracted dictionary of attributes, extract a natural-key ID for the feature.
    """

    feature_type = record.feature.lower()
    feature_id = attributes.get(GFF_ID_ATTR, (None,))[0]

    if feature_id:  # If the standardized GFF `ID` attribute is set, we can use it and skip any deriving logic.
        return feature_id

    match feature_type:
        case "gene":
            return attributes.get(GFF_GENCODE_GENE_ID_ATTR, (None,))[0]
        case "transcript":
            return attributes.get("transcript_id", (None,))[0]
        case "exon":
            return attributes.get("exon_id", (None,))[0]
        case _:  # no alternative ID attribute to use, so we couldn't figure anything out.
            return None


def extract_feature_name(record, attributes: dict[str, list[str]]) -> str | None:
    """
    Given a GFF3 record and an extracted dictionary of attributes, either extract or infer a (not necessarily unique)
    name for the feature.
    """

    feature_type = record.feature.lower()
    feature_name: str | None = attributes.get(GFF_NAME_ATTR, (None,))[0]

    if feature_name:  # If the standardized GFF `Name` attribute is set, we can use it and skip any deriving logic.
        return feature_name

    transcript_name = attributes.get("transcript_name", attributes.get("transcript_id", (None,)))[0]

    match feature_type:
        case "gene":
            return attributes.get("gene_name", (None,))[0]
        case "transcript":
            return transcript_name
        case "5utr" | "five_prime_utr":  # 5' untranslated region (UTR)
            return f"{transcript_name} 5' UTR" if transcript_name else None
        case "3utr" | "three_prime_utr":  # 3' untranslated region (UTR)
            return f"{transcript_name} 3' UTR" if transcript_name else None
        case "start_codon":
            return f"{transcript_name} start codon" if transcript_name else None
        case "stop_codon":
            return f"{transcript_name} stop codon" if transcript_name else None
        case "exon":
            exon_number = attributes.get("exon_number", (None,))[0]
            if transcript_name is None or exon_number is None:
                return None
            return f"{transcript_name} exon {exon_number}"
        case "cds":  # coding sequence
            return f"{transcript_name} CDS" if transcript_name else None
        case _:
            return None


def iter_features(
    # parameters:
    genome: m.Genome,
    gff_path: Path,
    gff_index_path: Path,
    # dependencies:
    logger: logging.Logger,
) -> Generator[tuple[m.GenomeFeature, ...], None, None]:
    """
    Given genome and a GFF3 for the genome, iterate through the lines of the GFF3 and build genome feature objects.
    """

    genome_id = genome.id

    total_processed: int = 0

    with pysam.TabixFile(str(gff_path), index=str(gff_index_path)) as gff:
        for contig in genome.contigs:
            contig_name = contig.name
            logger.info(f"Indexing features from contig {contig_name}")

            contig_features_by_id: dict[str, m.GenomeFeature] = {}

            try:
                fetch_iter = gff.fetch(reference=contig.name, parser=pysam.asGFF3())
            except ValueError as e:
                logger.warning(f"Could not find contig with name {contig_name} in GFF3; skipping... ({e})")
                continue

            for i, rec in enumerate(fetch_iter):
                feature_type = rec.feature

                if feature_type in GFF_SKIPPED_FEATURE_TYPES:
                    continue  # Don't ingest stop_codon_redefined_as_selenocysteine annotations

                # for some reason, dict(...) returns the attributes dict:
                feature_raw_attributes = dict(rec)

                try:
                    record_attributes = parse_attributes(feature_raw_attributes)

                    # - coordinates from PySAM are 0-based, semi-open
                    #    - to convert to 1-based semi-open coordinates like in the original GFF3, we add 1 to start
                    #      (we should have to add 1 to end too, but the GFF3 parser is busted in PySAM I guess, so we
                    #       leave it as-is)
                    start_pos = rec.start + 1
                    end_pos = rec.end

                    feature_id = extract_feature_id(rec, record_attributes)
                    if feature_id is None:
                        logger.warning(
                            f"Skipping unsupported feature {i}: type={feature_type}, no ID retrieval; "
                            f"{contig_name}:{start_pos}-{end_pos}"
                        )
                        continue

                    feature_name = extract_feature_name(rec, record_attributes)
                    if feature_name is None:
                        logger.warning(
                            f"Using ID as name for feature {i}: {feature_id} {contig_name}:{start_pos}-{end_pos}"
                        )
                        feature_name = feature_id

                    entry = m.GenomeFeatureEntry(
                        start_pos=start_pos,
                        end_pos=end_pos,
                        score=rec.score,
                        # - 'phase' is misnamed / legacy-named as 'frame' in PySAM's GFF3 parser
                        phase=rec.frame,
                    )

                    if feature_id in contig_features_by_id:
                        contig_features_by_id[feature_id].entries.append(entry)
                    else:
                        attributes: dict[str, list[str]] = {
                            # skip attributes which have been captured in the above information:
                            k: vs
                            for k, vs in record_attributes.items()
                            if k not in GFF_CAPTURED_ATTRIBUTES
                        }

                        contig_features_by_id[feature_id] = m.GenomeFeature(
                            genome_id=genome_id,
                            contig_name=contig_name,
                            strand=rec.strand or ".",  # None/"." <=> unstranded
                            feature_id=feature_id,
                            feature_name=feature_name,
                            feature_type=feature_type,
                            source=rec.source,
                            entries=[entry],
                            gene_id=record_attributes.get(GFF_GENCODE_GENE_ID_ATTR, (None,))[0],
                            attributes=attributes,
                            parents=tuple(p for p in record_attributes.get(GFF_PARENT_ATTR, ()) if p),
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

            yield tuple(contig_features_by_id.values())


async def ingest_features(
    # parameters:
    genome: m.GenomeWithURIs,
    gff_path: Path,
    gff_index_path: Path,
    # dependencies:
    db: Database,
    logger: logging.Logger,
) -> int:
    """
    Given a genome ID and a path to an external GTF gene/exon/transcript annotation file, this function copies the GTF
    into the relevant .bentoGenome directory and ingests the annotations into an ElasticSearch index for fuzzy text
    querying of features.
    :param genome: The genome to attach the annotation to.
    :param gff_path: The path to an external GTF.gz-formatted annotation file to copy and read from.
    :param gff_index_path: The path to an external index file for the above .gtf.gz.
    :param db: Database connection/management object.
    :param logger: Python logger object.
    :return: None
    """

    logger.info(f"Ingesting gene features for genome {genome.id}...")

    features_to_ingest = iter_features(genome, gff_path, gff_index_path, logger)
    n_ingested: int = 0

    # take features in contig batches
    #  - these contig batches are created by the generator produced iter_features(...)
    #  - we use contigs as batches rather than a fixed batch size so that we are guaranteed to get parents alongside
    #    their child features in the same batch, so we can assign surrogate keys correctly.
    while data := next(features_to_ingest, ()):
        s = datetime.now()
        logger.debug(f"ingest_gene_feature_annotation: ingesting batch of {len(data)} features")
        await db.bulk_ingest_genome_features(data)
        n_ingested += len(data)
        logger.debug(f"ingest_gene_feature_annotation: batch took {(datetime.now() - s).total_seconds():.1f} seconds")

    if n_ingested == 0:
        raise AnnotationIngestError("No gene features could be ingested - is this a valid GFF3 file?")

    logger.info(f"ingest_gene_feature_annotation: ingested {n_ingested} gene features")

    return n_ingested


async def download_uri_into_temporary_file(uri: str, tmp: Path, config: Config, logger: logging.Logger):
    logger.debug(f"Saving data from URI {uri} into temporary file {tmp}")

    _, stream_iter = await stream_from_uri(config, logger, uri, range_header=None, impose_response_limit=False)

    # copy .gff3.gz to temporary directory for ingestion
    async with aiofiles.open(tmp, "wb") as fh:
        while data := (await anext(stream_iter, None)):
            await fh.write(data)

    logger.debug(f"Wrote downloaded data to {tmp}; size={tmp.stat().st_size}")


async def download_feature_files(genome: m.GenomeWithURIs, config: Config, logger: logging.Logger):
    tmp_file_id = str(uuid4())

    if genome.gff3_gz is None:
        raise AnnotationIngestError(f"Genome {genome.id} is missing a GFF3 file")
    if genome.gff3_gz_tbi is None:
        raise AnnotationIngestError(f"Genome {genome.id} is missing a GFF3 Tabix index")

    fn = config.file_ingest_tmp_dir / f"{tmp_file_id}.gff3.gz"
    await download_uri_into_temporary_file(genome.gff3_gz, fn, config, logger)

    fn_tbi = config.file_ingest_tmp_dir / f"{tmp_file_id}.gff3.gz.tbi"
    await download_uri_into_temporary_file(genome.gff3_gz_tbi, fn_tbi, config, logger)

    return fn, fn_tbi


async def ingest_features_task(genome_id: str, task_id: int, config: Config, db: Database, logger: logging.Logger):
    # the ingest_features task moves from queued -> running -> (success | error)

    await db.update_task_status(task_id, "running")

    genome: m.GenomeWithURIs | None = await db.get_genome(genome_id)
    if genome is None:
        err = f"task {task_id}: genome with ID {genome_id} not found"
        logger.error(err)
        await db.update_task_status(task_id, "error", message=err)
        raise AnnotationGenomeNotFoundError(err)

    try:
        # download GFF3 + GFF3 TBI file for this genome
        logger.info(f"Downloading gene feature files for genome {genome_id}")
        gff3_gz_path, gff3_gz_tbi_path = await download_feature_files(genome, config, logger)
    except Exception as e:
        err = (
            f"task {task_id}: encountered exception while downloading feature files: {e}; traceback: "
            f"{traceback.format_exc()}"
        )
        logger.error(err)
        await db.update_task_status(task_id, "error", message=err)
        raise AnnotationIngestError(err)

    try:
        # clear existing gene features for this genome
        logger.info(f"Clearing gene features for genome {genome_id} in preparation for feature (re-)ingestion")
        await db.clear_genome_features(genome_id)

        # ingest gene features into the database
        n_ingested = await ingest_features(genome, gff3_gz_path, gff3_gz_tbi_path, db, logger)
        await db.update_task_status(task_id, "success", message=f"ingested {n_ingested} features")

    except Exception as e:
        err = (
            f"task {task_id}: encountered exception while ingesting features: {e}; traceback: {traceback.format_exc()}"
        )
        logger.error(err)
        await db.update_task_status(task_id, "error", message=err)

    finally:
        # unlink temporary files
        gff3_gz_path.unlink(missing_ok=True)
        gff3_gz_tbi_path.unlink(missing_ok=True)
