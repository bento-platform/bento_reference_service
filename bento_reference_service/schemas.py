from jsonschema import Draft202012Validator
from typing import Dict, List, TypedDict

from bento_lib.search import queries as q
from .config import config

__all__ = [
    "schema_uri",
    "ALIAS_SCHEMA",
    "CONTIG_SCHEMA",
    "GENOME_METADATA_SCHEMA",
    "GENOME_METADATA_SCHEMA_VALIDATOR",
    "SCHEMAS_BY_FILE_NAME",
]


def schema_uri(path: str) -> str:
    return f"{config.service_url_base_path.rstrip('/')}/schemas/{path}"


TDJSONSchema = TypedDict("TDJSONSchema", {
    "$id": str,
    "$schema": str,
    "title": str,
    "type": str,
    "properties": Dict[str, dict],
    "required": List[str],
}, total=False)


def search_optional_eq(order: int):
    return {
        "operations": [q.SEARCH_OP_EQ, q.SEARCH_OP_IN],
        "queryable": "all",
        "canNegate": True,
        "required": False,
        "order": order,
        "type": "single",
    }


def search_optional_str(order: int):
    return {
        "operations": [
            q.SEARCH_OP_EQ,
            q.SEARCH_OP_ICO,
            q.SEARCH_OP_IN,
            q.SEARCH_OP_ISW,
            q.SEARCH_OP_IEW,
        ],
        "queryable": "all",
        "canNegate": True,
        "required": False,
        "order": order,
        "type": "single",
    }


ONTOLOGY_TERM_SCHEMA: TDJSONSchema = {
    "$id": schema_uri("ontology_term.json"),
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Ontology Term",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
            "search": search_optional_eq(0),
        },
        "label": {
            "type": "string",
            "search": search_optional_str(1),
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
            "search": search_optional_str(0),
        },
        "naming_authority": {
            "type": "string",
            "search": search_optional_str(1),
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
            "search": search_optional_eq(0),
        },
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
            "search": {"order": 1},
        },
        "md5": {
            "type": "string",
            "search": search_optional_eq(2),
        },
        "trunc512": {
            "type": "string",
            "search": search_optional_eq(3),
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
            "search": search_optional_eq(0),
        },
        "aliases": {
            "type": "array",
            "items": ALIAS_SCHEMA,
            "search": {"order": 1},
        },
        "md5": {
            "type": "string",
            "search": search_optional_eq(2),
        },
        "trunc512": {
            "type": "string",
            "search": search_optional_eq(3),
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


SCHEMAS_BY_FILE_NAME: Dict[str, TDJSONSchema] = {
    "ontology_term.json": ONTOLOGY_TERM_SCHEMA,
    "alias_schema.json": ALIAS_SCHEMA,
    "contig_schema.json": CONTIG_SCHEMA,
    "genome_metadata.json": GENOME_METADATA_SCHEMA,
}
