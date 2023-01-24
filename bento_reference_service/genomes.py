import aiofiles
import json
import pysam

from .config import config
from .logger import logger
from .models import Alias, Contig, Genome

from typing import List


async def get_genomes() -> List[Genome]:
    genomes: List[Genome] = []

    for genome in config.data_path.glob("*.bentoGenome"):
        if not genome.is_dir():
            logger.error(f"Skipping {genome}: not a directory")
            continue

        metadata_path = genome / "metadata.json"
        sequence_path = genome / "sequence.fa"
        sequence_index_path = genome / "sequence.fa.fai"

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

        # TODO: calculate checksums (?) will be slow - cache response

        fa = pysam.FastaFile(str(sequence_path), filepath_index=str(sequence_index_path))
        contigs: List[Contig] = []
        try:
            for contig in fa.references:
                contigs.append(Contig(
                    name=contig,
                    aliases=[],  # TODO: get from metadata if provided
                    md5="TODO",  # TODO: calculate or some have it embedded...
                    trunc512="TODO",  # TODO
                ))
        finally:
            fa.close()

        genomes.append(Genome(
            id=genome_metadata["id"],
            aliases=[Alias(**alias) for alias in genome_metadata.get("aliases", [])],
            contigs=contigs,
            md5="TODO",
            trunc512="TODO",
        ))

    return genomes
