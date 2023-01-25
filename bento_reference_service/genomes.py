import aiofiles
import json
import pysam

from pathlib import Path
from typing import List, Generator

from .config import config
from .logger import logger
from .models import Alias, Contig, Genome
from .utils import make_uri

__all__ = [
    "ingest_genome",
    "get_genomes",
]


async def ingest_genome(genome_id: str, bento_genome_path: Path) -> None:
    """
    Given an external directory following the ".bentoGenome" directory specification, this function validates the format
    and copies the directory into the local data folder.
    :param genome_id: The ID of the genome; the .bentoGenome will be renamed to <genome_id>.bentoGenome.
    :param bento_genome_path: The path to the .bentoGenome directory.
    :return: None
    """

    # TODO: ingest genome & write path
    # TODO: QC checks

    pass


async def ingest_gene_feature_annotation(genome_id: str, gtf_annotation_path: Path) -> None:
    """
    Given a genome ID and a path to an external GTF gene/exon/transcript annotation file, this function copies the GTF
    into the relevant .bentoGenome directory and ingests the annotations into an ElasticSearch index for fuzzy text
    querying of features.
    :param genome_id: The ID of the genome to attach the annotation to.
    :param gtf_annotation_path: The path to an external GTF-formatted annotation file to copy and read from.
    :return: None
    """

    # TODO
    pass


async def get_genomes() -> Generator[Genome, None, None]:
    for genome in config.data_path.glob("*.bentoGenome"):
        if not genome.is_dir():
            logger.error(f"Skipping {genome}: not a directory")
            continue

        metadata_path = (genome / "metadata.json").resolve().absolute()
        sequence_path = (genome / "sequence.fa").resolve().absolute()
        sequence_index_path = (genome / "sequence.fa.fai").resolve().absolute()

        # Skip any genomes which are missing one of the above required files
        skip_genome: bool = False
        for required_file in (metadata_path, sequence_path, sequence_index_path):
            if not required_file.exists():
                logger.error(f"Skipping {genome}: missing required file {required_file}")
                skip_genome = True

            elif required_file.is_dir():
                logger.error(f"Skipping {genome}: {required_file} should not be a directory")
                skip_genome = True

        if skip_genome:
            continue

        async with aiofiles.open(metadata_path, "r") as mf:
            genome_metadata = json.loads(await mf.read())

        id_ = genome_metadata["id"]
        correct_name = f"{id_}.bentoGenome"

        # Skip genomes where the directory name != the ID specified in metadata.json, plus .bentoGenome as a suffix
        if genome.name != correct_name:
            logger.error(f"Skipping {genome}: mismatch between directory name and ID-derived name ({correct_name})")
            continue

        # Extract contigs from the FASTA file, and tag them with the checksums provided in the metadata JSON
        fa = pysam.FastaFile(str(sequence_path), filepath_index=str(sequence_index_path))
        contigs: List[Contig] = []
        try:
            for contig in fa.references:
                gm_contig = genome_metadata["contigs"][contig]
                contig_aliases: List[Alias] = [Alias(**ca) for ca in gm_contig.get("aliases", [])]
                contigs.append(Contig(
                    name=contig,
                    aliases=contig_aliases,
                    md5=genome_metadata["contigs"][contig]["md5"],
                    trunc512=genome_metadata["contigs"][contig]["trunc512"],
                ))
        finally:
            fa.close()

        yield Genome(
            id=id_,
            aliases=[Alias(**alias) for alias in genome_metadata.get("aliases", [])],
            uri=make_uri(f"/genomes/{id_}"),
            contigs=contigs,
            md5=genome_metadata["md5"],
            trunc512=genome_metadata["trunc512"],
            fasta=sequence_path,
            fai=sequence_index_path,
        )
