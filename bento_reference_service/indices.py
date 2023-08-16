import re
from functools import lru_cache
from typing import TypedDict
from .config import Config

__all__ = [
    "BentoESIndex",
    "make_genome_index_def",
    "make_gene_feature_index_def",
]

ES_INVALID_INDEX_CHARS = re.compile(r"[/\\*?\"<>| ,#:]")


class BentoESIndex(TypedDict):
    name: str
    mappings: dict[str, str | dict]


_alias_type = {
    "type": "nested",
    "properties": {
        "alias": {"type": "keyword"},
        "naming_authority": {"type": "text"},
    },
}


def make_sanitized_service_id(config: Config) -> str:
    return ES_INVALID_INDEX_CHARS.sub("_", config.service_id.lower())


@lru_cache()
def make_genome_index_def(config: Config) -> BentoESIndex:
    return BentoESIndex(
        name=f"{make_sanitized_service_id(config)}.genomes",
        mappings={  # TODO: generate from models automatically or something
            "id": {"type": "keyword"},
            "aliases": _alias_type,
            "md5": {"type": "text"},
            "trunc512": {"type": "text"},
            "contigs": {
                "type": "nested",
                "properties": {
                    "name": {"type": "keyword"},  # contig name, e.g. chr1
                    "aliases": _alias_type,
                    "md5": {"type": "text"},
                    "trunc512": {"type": "text"},
                },
            },
            "fasta": {"type": "text"},
            "fai": {"type": "text"},
        },
    )


@lru_cache()
def make_gene_feature_index_def(config: Config) -> BentoESIndex:
    return BentoESIndex(
        name=f"{make_sanitized_service_id(config)}.gene_features",
        mappings={
            # ID for the type - if none is available, this is synthesized from the gene–id + '-' + type
            #  - (KIR3DL3 + 5UTR --> KIR3DL3-5UTR)
            "id": {
                "type": "text",
                "analyzer": "standard",
            },  # gene_id or transcript_id or exon_id from attributes list
            # name (or ID if name is not available for the item)
            #  - if no name or ID is available for this type, name is set to gene_name + ' ' + human-readable type name
            #    (5UTR for KIR3DL3 --> KIR3DL3 5' UTR)
            "name": {"type": "search_as_you_type"},
            # contig:start-end - turn into searchable text
            "position": {"type": "text", "analyzer": "standard"},
            "type": {"type": "keyword"},  # feature type - gene/exon/transcript/5UTR/3UTR/...
            "genome": {"type": "keyword"},  # genome ID
            "strand": {"type": "keyword"},  # strand + or - TODO
        },
    )
