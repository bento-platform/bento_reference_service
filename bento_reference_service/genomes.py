import aiofiles
import asyncio
import logging
import pysam
import re

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk
from fastapi import HTTPException
from pathlib import Path
from typing import Generator

from . import models as m
from .config import Config
from .es import ESDependency
from .indices import make_genome_index_def
from .utils import make_uri

__all__ = [
    "make_genome_path",
    "ingest_genome",
    "ingest_gene_feature_annotation",
    "get_genome",
    "get_genome_or_error",
    "get_genome_with_uris",
    "get_genome_with_uris_or_error",
    "get_genomes_with_uris",
]


VALID_GENOME_ID = re.compile(r"^[a-zA-Z0-9\-_.]{3,50}$")


class GenomeFormatError(Exception):
    pass


class AnnotationIngestError(Exception):
    pass


def make_genome_dir_name(id_: str) -> str:
    return f"{id_}.bentoGenome"


def make_genome_path(id_: str, config: Config) -> Path:
    return (config.data_path / make_genome_dir_name(id_)).absolute().resolve()


def get_genome_paths(genome: Path) -> tuple[Path, Path, Path]:
    return (
        (genome / "metadata.json").resolve().absolute(),
        (genome / "sequence.fa").resolve().absolute(),
        (genome / "sequence.fa.fai").resolve().absolute(),
    )


def check_genome_file_existence(required_files: tuple[Path, ...]) -> None:
    # Raise errors with genomes which are missing one of the above required files
    for required_file in required_files:
        if not required_file.exists():
            raise GenomeFormatError(f"missing required file {required_file}")

        elif required_file.is_dir():
            raise GenomeFormatError(f"{required_file} should not be a directory")


def get_genome_paths_and_check(genome_path: Path) -> tuple[Path, Path, Path]:
    genome_paths = get_genome_paths(genome_path)

    # Raise errors with genomes which are missing a required file
    check_genome_file_existence(genome_paths)

    return genome_paths


async def ingest_genome(bento_genome_path: Path, config: Config, es: AsyncElasticsearch) -> m.Genome:
    """
    Given an external directory following the ".bentoGenome" directory specification, this function validates the format
    and copies the directory into the local data folder.
    :param bento_genome_path: The path to the .bentoGenome directory.
    :param config: Service configuration object.
    :param es: Async Elasticsearch connection.
    :return: The created genome object
    """

    if not bento_genome_path.is_dir():
        raise GenomeFormatError(f"{bento_genome_path} is not a directory")

    metadata_path, sequence_path, sequence_index_path = get_genome_paths_and_check(bento_genome_path)

    # Read ID from metadata.json
    async with aiofiles.open(metadata_path, "r") as mf:
        genome = m.Genome.model_validate_json(await mf.read())

    id_ = genome.id

    # Check if the genome ID is valid - ensures no funky file names
    if not VALID_GENOME_ID.match(id_):
        raise GenomeFormatError(f"invalid genome ID: {id_} (must match /{VALID_GENOME_ID.pattern}/)")

    # Copy the genome directory to service storage
    copy_proc = await asyncio.create_subprocess_exec(
        "cp",
        str(bento_genome_path.absolute().resolve()),
        str(make_genome_path(id_, config)),
    )
    stdout, stderr = await copy_proc.communicate()
    if (rc := copy_proc.returncode) != 0:
        raise GenomeFormatError(f"on copy, got non-0 return code {rc} (stdout={stdout}, stderr={stderr})")

    # Index the genome in ES - it may not be available immediately
    genome_index = make_genome_index_def(config)
    await es.create(index=genome_index["name"], document=genome.model_dump(mode="json"))

    return genome


async def ingest_gene_feature_annotation(
    genome_id: str,
    gtf_annotation_path: Path,
    gtf_annotation_index_path: Path,
    config: Config,
    es: ESDependency,
    logger: logging.Logger,
) -> None:
    """
    Given a genome ID and a path to an external GTF gene/exon/transcript annotation file, this function copies the GTF
    into the relevant .bentoGenome directory and ingests the annotations into an ElasticSearch index for fuzzy text
    querying of features.
    :param genome_id: The ID of the genome to attach the annotation to.
    :param gtf_annotation_path: The path to an external GTF.gz-formatted annotation file to copy and read from.
    :param gtf_annotation_index_path: The path to an external index file for the above .gtf.gz.
    :param config: Service configuration object.
    :param es: Async Elasticsearch connection.
    :param logger: Python logger object.
    :return: None
    """

    # TODO: make sure it's a gtf.gz
    # TODO: copy it in place

    genome: m.Genome = await get_genome(make_genome_path(genome_id, config), config)

    log_progress_interval = 1000

    def _iter_features() -> Generator[m.GTFFeature, None, None]:
        gtf = pysam.TabixFile(str(gtf_annotation_path), index=str(gtf_annotation_index_path))
        total_processed: int = 0
        try:
            for contig in genome.contigs:
                logger.info(f"Indexing features from contig {contig.name}")

                for record in gtf.fetch(contig.name, parser=pysam.asGTF()):
                    feature_type = record.feature
                    gene_id = record.gene_id
                    gene_name = record.attributes.get("gene_name", gene_id)

                    feature_id: str | None = None
                    feature_name: str | None = None

                    if feature_type == "gene":
                        feature_id = gene_id
                        feature_name = gene_name
                    elif feature_type == "transcript":
                        feature_id = record.transcript_id
                        feature_name = feature_id  # Explicitly re-use ID as name here
                    elif feature_type in (
                        "5UTR",
                        "five_prime_utr",
                    ):  # 5' untranslated region (UTR)
                        feature_id = f"{gene_id}-5UTR"
                        feature_name = f"{gene_name} 5' UTR"
                    elif feature_type in (
                        "3UTR",
                        "five_prime_utr",
                    ):  # 3' untranslated region (UTR)
                        feature_id = f"{gene_id}-3UTR"
                        feature_name = f"{gene_name} 3' UTR"
                    elif feature_type == "start_codon":  # TODO: multiple start codons may exist?
                        feature_id = f"{gene_id}-start_codon"
                        feature_name = f"{gene_name} start codon"
                    elif feature_type == "stop_codon":  # TODO: multiple stop codons may exist?
                        feature_id = f"{gene_id}-stop_codon"
                        feature_name = f"{gene_name} stop codon"
                    elif feature_type == "exon":
                        feature_id = record.attributes["exon_id"]  # TODO: fallback with gene ID + exon number?
                        if "exon_number" in record.attributes:
                            # TODO: Validate this, I think slightly wrong because it uses gene vs. transcript
                            feature_name = f"{gene_name} exon {record.attributes['exon_number']}"
                        else:
                            feature_name = feature_id  # Explicitly re-use ID as name here
                    elif feature_type == "CDS":  # coding sequence
                        exon_id = record.attributes["exon_id"]
                        feature_id = f"{exon_id}-CDS"
                        if "exon_number" in record.attributes:
                            # TODO: Validate this, I think slightly wrong because it uses gene vs. transcript
                            feature_name = f"{gene_name} exon {record.attributes['exon_number']} CDS"
                        else:
                            feature_name = f"{exon_id} CDS"  # Explicitly re-use ID as name here

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
                        "type": record["feature"],
                        "genome": genome_id,
                        "strand": record["strand"],
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


def contig_with_refget_uri(contig: m.Contig, config: Config) -> m.ContigWithRefgetURI:
    return m.ContigWithRefgetURI.model_validate(
        {
            **contig.model_dump(),
            "refget": make_uri(f"/sequences/{contig.trunc512}", config),
        }
    )


async def get_genome(genome_path: Path, config: Config) -> m.Genome:
    if not genome_path.is_dir():
        raise GenomeFormatError("not a directory")

    if not genome_path.parent.absolute().resolve() == config.data_path.absolute().resolve():
        raise GenomeFormatError("invalid location")

    # Raise errors with genomes which are missing a required file
    metadata_path, _, _ = get_genome_paths_and_check(genome_path)

    async with aiofiles.open(metadata_path, "r") as mf:
        genome: m.Genome = m.Genome.model_validate_json(await mf.read())

    # Error on genomes where the directory name != the ID specified in metadata.json, plus .bentoGenome as a suffix
    if genome.name != (correct_name := make_genome_dir_name(genome.id)):
        raise GenomeFormatError(f"mismatch between directory name ({genome}) and ID-derived name ({correct_name})")

    return genome


async def get_genome_with_uris(genome_path: Path, config: Config):
    genome = await get_genome(genome_path, config)

    contigs: list[m.ContigWithRefgetURI] = [contig_with_refget_uri(contig, config) for contig in genome.contigs]

    id_ = genome.id
    return m.GenomeWithURIs.model_validate(
        {
            **genome.model_dump(),
            "contigs": contigs,
            "uri": make_uri(f"/genomes/{id_}", config),
            "fasta": make_uri(f"/genomes/{id_}.fa", config),
            "fai": make_uri(f"/genomes/{id_}.fa.fai", config),
        }
    )


def make_and_check_genome_path(genome_id: str, config: Config):
    genome_path = make_genome_path(genome_id, config)

    if not genome_path.exists():
        raise HTTPException(status_code=404, detail=f"genome not found: {genome_id}")

    return genome_path


async def get_genome_or_error(genome_id: str, config: Config) -> m.Genome:
    # TODO: handle format errors with 500
    return await get_genome(make_and_check_genome_path(genome_id, config), config)


async def get_genome_with_uris_or_error(genome_id: str, config: Config) -> m.GenomeWithURIs:
    # TODO: handle format errors with 500
    return await get_genome_with_uris(make_and_check_genome_path(genome_id, config), config)


async def get_genomes_with_uris(config: Config, logger: logging.Logger) -> Generator[m.GenomeWithURIs, None, None]:
    for genome in config.data_path.glob("*.bentoGenome"):
        try:
            yield await get_genome_with_uris(genome, config)
        except GenomeFormatError as e:
            logger.error(f"Skipping {genome}: {e}")
            continue
