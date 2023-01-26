import aiofiles
import asyncio
import gffutils
import json
import pysam
import re

from pathlib import Path
from typing import List, Generator, Tuple

from .config import config
from .es import es
from .logger import logger
from .models import Alias, Contig, Genome
from .utils import make_uri

__all__ = [
    "make_genome_path",
    "ingest_genome",
    "ingest_gene_feature_annotation",
    "get_genome",
    "get_genomes",
]


VALID_GENOME_ID = re.compile(r"^[a-zA-Z0-9\-_.]{3,50}$")


class GenomeFormatError(Exception):
    pass


def make_genome_dir_name(id_: str) -> str:
    return f"{id_}.bentoGenome"


def make_genome_path(id_: str) -> Path:
    return (config.data_path / make_genome_dir_name(id_)).absolute().resolve()


def get_genome_paths(genome: Path) -> Tuple[Path, Path, Path]:
    return (
        (genome / "metadata.json").resolve().absolute(),
        (genome / "sequence.fa").resolve().absolute(),
        (genome / "sequence.fa.fai").resolve().absolute(),
    )


def check_genome_file_existence(required_files: Tuple[Path, ...]) -> None:
    # Raise errors with genomes which are missing one of the above required files
    for required_file in required_files:
        if not required_file.exists():
            raise GenomeFormatError(f"missing required file {required_file}")

        elif required_file.is_dir():
            raise GenomeFormatError(f"{required_file} should not be a directory")


async def ingest_genome(bento_genome_path: Path) -> None:
    """
    Given an external directory following the ".bentoGenome" directory specification, this function validates the format
    and copies the directory into the local data folder.
    :param bento_genome_path: The path to the .bentoGenome directory.
    :return: None
    """

    # TODO: more QC checks
    # TODO: index genome & contigs

    if not bento_genome_path.is_dir():
        raise GenomeFormatError(f"{bento_genome_path} is not a directory")

    genome_paths = get_genome_paths(bento_genome_path)

    # Check required file existence
    check_genome_file_existence(genome_paths)

    metadata_path, sequence_path, sequence_index_path = genome_paths

    # Read ID from metadata.json
    async with aiofiles.open(metadata_path, "r") as mf:
        genome_metadata = json.loads(await mf.read())
    id_ = genome_metadata["id"]

    # Check if the genome ID is valid - ensures no funky file names
    if not VALID_GENOME_ID.match(id_):
        raise GenomeFormatError(f"invalid genome ID: {id_} (must match /{VALID_GENOME_ID.pattern}/)")

    dest_path = make_genome_path(id_)

    copy_proc = await asyncio.create_subprocess_exec(
        "cp",
        str(bento_genome_path.absolute().resolve()),
        str(dest_path),
    )

    stdout, stderr = await copy_proc.communicate()

    if (rc := copy_proc.returncode) != 0:
        raise GenomeFormatError(f"on copy, got non-0 return code {rc} (stdout={stdout}, stderr={stderr})")


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


async def get_genome(genome: Path) -> Genome:
    if not genome.is_dir():
        raise GenomeFormatError("not a directory")

    if not genome.parent.absolute().resolve() == config.data_path.absolute().resolve():
        raise GenomeFormatError("invalid location")

    genome_paths = get_genome_paths(genome)

    # Raise errors with genomes which are missing a required file
    check_genome_file_existence(genome_paths)

    metadata_path, sequence_path, sequence_index_path = genome_paths

    async with aiofiles.open(metadata_path, "r") as mf:
        genome_metadata = json.loads(await mf.read())

    id_ = genome_metadata["id"]
    correct_name = make_genome_dir_name(id_)

    # Error on genomes where the directory name != the ID specified in metadata.json, plus .bentoGenome as a suffix
    if genome.name != correct_name:
        raise GenomeFormatError(f"mismatch between directory name ({genome}) and ID-derived name ({correct_name})")

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

    return Genome(
        id=id_,
        aliases=[Alias(**alias) for alias in genome_metadata.get("aliases", [])],
        uri=make_uri(f"/genomes/{id_}"),
        contigs=contigs,
        md5=genome_metadata["md5"],
        trunc512=genome_metadata["trunc512"],
        fasta=sequence_path,
        fai=sequence_index_path,
    )


async def get_genomes() -> Generator[Genome, None, None]:
    for genome in config.data_path.glob("*.bentoGenome"):
        try:
            yield await get_genome(genome)
        except GenomeFormatError as e:
            logger.error(f"Skipping {genome}: {e}")
            continue
