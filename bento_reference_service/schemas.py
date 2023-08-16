from jsonschema import Draft202012Validator
from typing import TypedDict

from .config import Config

__all__ = [
    "schema_uri",
    "ALIAS_SCHEMA",
    "CONTIG_SCHEMA",
    "GENOME_METADATA_SCHEMA",
    "GENOME_METADATA_SCHEMA_VALIDATOR",
    "SCHEMAS_BY_FILE_NAME",
]


def schema_uri(path: str, config: Config) -> str:
    return f"{config.service_url_base_path.rstrip('/')}/schemas/{path}"


TDJSONSchema = TypedDict(
    "TDJSONSchema",
    {
        "$id": str,
        "$schema": str,
        "title": str,
        "type": str,
        "properties": dict[str, dict],
        "required": list[str],
    },
    total=False,
)


ONTOLOGY_TERM_SCHEMA: TDJSONSchema = {
    "$id": schema_uri("ontology_term.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Ontology Term",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
        },
        "label": {
            "type": "string",
        },
    },
    "required": ["id", "label"],
}


ALIAS_SCHEMA: TDJSONSchema = {
    "$id": schema_uri("alias.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Alias",
    "type": "object",
    "properties": {
        "alias": {
            "type": "string",
        },
        "naming_authority": {
            "type": "string",
        },
    },
    "required": ["alias", "naming_authority"],
}


CONTIG_SCHEMA: TDJSONSchema = {
    "$id": schema_uri("contig.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Contig",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
        },
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
            "search": {"order": 1},
        },
        "md5": {
            "type": "string",
        },
        "trunc512": {
            "type": "string",
        },
        "length": {"type": "integer", "minimum": 0},
    },
    "required": ["name", "md5", "trunc512"],
}


GENOME_METADATA_SCHEMA: TDJSONSchema = {
    "$id": schema_uri("genome_metadata.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Genome Metadata",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
        },
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
            "search": {"order": 1},
        },
        "md5": {
            "type": "string",
        },
        "trunc512": {
            "type": "string",
        },
        "taxon": {**ONTOLOGY_TERM_SCHEMA, "search": {"order": 4}},
        "contigs": {
            "type": "array",
            "items": CONTIG_SCHEMA,
            "search": {"order": 5},
        },
        "fasta": {"type": "string"},  # Path or URI
        "fai": {"type": "string"},  # Path or URI
    },
    "required": ["id", "md5", "trunc512", "contigs"],
}
GENOME_METADATA_SCHEMA_VALIDATOR = Draft202012Validator(GENOME_METADATA_SCHEMA)


SCHEMAS_BY_FILE_NAME: dict[str, TDJSONSchema] = {
    "ontology_term.json": ONTOLOGY_TERM_SCHEMA,
    "alias_schema.json": ALIAS_SCHEMA,
    "contig_schema.json": CONTIG_SCHEMA,
    "genome_metadata.json": GENOME_METADATA_SCHEMA,
}
