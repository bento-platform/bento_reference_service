__all__ = [
    "alias_schema",
    "contig_schema",
    "genome_metadata_schema",
]


def alias_schema(base_uri: str):
    return {
        "$id": f"{base_uri.rstrip('/')}/alias.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Alias",
        "type": "object",
        "properties": {
            "alias": {"type": "string"},
            "naming_authority": {"type": "string"},
        },
        "required": ["alias", "naming_authority"],
    }


def contig_schema(base_uri: str):
    return {
        "$id": f"{base_uri.rstrip('/')}/contig.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Contig",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "aliases": {
                "type": "array",
                "items": alias_schema(base_uri),
            },
            "md5": {"type": "string"},
            "trunc512": {"type": "string"},
        },
        "required": ["name", "md5", "trunc512"],
    }


def genome_metadata_schema(base_uri: str):
    return {
        "$id": f"{base_uri.rstrip('/')}/genome_metadata.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Genome Metadata",
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "aliases": {
                "type": "array",
                "items": alias_schema(base_uri),
            },
            "md5": {"type": "string"},
            "trunc512": {"type": "string"},
            "contigs": {
                "type": "array",
                "items": contig_schema(base_uri),
            },
            "fasta": {"type": "string"},  # Path or URI
            "fai": {"type": "string"},  # Path or URI
        },
        "required": ["id", "md5", "trunc512", "contigs", "fasta", "fai"],
    }
