import re
from typing import Tuple, TypedDict, Union
from .config import config

__all__ = [
    "BentoESIndex",
    "genome_index",
    "gene_feature_index",
    "ALL_INDICES",
]

ES_INVALID_INDEX_CHARS = re.compile(r"[/\\*?\"<>| ,#:]")


class BentoESIndex(TypedDict):
    name: str
    mappings: dict[str, Union[str, dict]]


# TODO: More thorough regex
sanitized_service_id = ES_INVALID_INDEX_CHARS.sub("_", config.service_id.lower())

_alias_type = {
    "type": "nested",
    "properties": {
        "alias": {"type": "keyword"},
        "naming_authority": {"type": "text"},
    },
}

genome_index = BentoESIndex(
    name=f"{sanitized_service_id}.genomes",
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

gene_feature_index = BentoESIndex(
    name=f"{sanitized_service_id}.gene_features",
    mappings={
        # ID for the type - if none is available, this is synthesized from the gene–id + '-' + type
        #  - (KIR3DL3 + 5UTR --> KIR3DL3-5UTR)
        "id": {"type": "text", "analyzer": "standard"},  # gene_id or transcript_id or exon_id from attributes list
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

ALL_INDICES: Tuple[BentoESIndex, ...] = (
    genome_index,
    gene_feature_index,
)

