from jsonschema import Draft202012Validator
from .config import config

__all__ = [
    "schema_uri",
    "ALIAS_SCHEMA",
    "CONTIG_SCHEMA",
    "GENOME_METADATA_SCHEMA",
    "GENOME_METADATA_SCHEMA_VALIDATOR",
]


def schema_uri(path: str) -> str:
    return f"{config.service_url_base_path.rstrip('/')}/schemas/{path}"


ALIAS_SCHEMA = {
    "$id": schema_uri("alias.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Alias",
    "type": "object",
    "properties": {
        "alias": {"type": "string"},
        "naming_authority": {"type": "string"},
    },
    "required": ["alias", "naming_authority"],
}


CONTIG_SCHEMA = {
    "$id": schema_uri("contig.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Contig",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
        },
        "md5": {"type": "string"},
        "trunc512": {"type": "string"},
    },
    "required": ["name", "md5", "trunc512"],
}


GENOME_METADATA_SCHEMA = {
    "$id": schema_uri("genome_metadata.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Genome Metadata",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
        },
        "md5": {"type": "string"},
        "trunc512": {"type": "string"},
        "contigs": {
            "type": "array",
            "items": CONTIG_SCHEMA,
        },
        "fasta": {"type": "string"},  # Path or URI
        "fai": {"type": "string"},  # Path or URI
    },
    "required": ["id", "md5", "trunc512", "contigs", "fasta", "fai"],
}
GENOME_METADATA_SCHEMA_VALIDATOR = Draft202012Validator(GENOME_METADATA_SCHEMA)
