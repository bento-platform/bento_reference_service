from .config import config

__all__ = [
    "gene_feature_index_name",
    "gene_feature_mappings",
]


# TODO: More thorough regex
sanitized_service_id = config.service_id.replace(":", "_")

gene_feature_index_name = f"{sanitized_service_id}.gene_features"
gene_feature_mappings = {
    # ID for the type - if none is available, this is synthesized from the gene–id + '-' + type
    #  - (KIR3DL3 + 5UTR --> KIR3DL3-5UTR)
    "id": {"type": "text", "analyzer": "standard"},  # gene_id or transcript_id or exon_id from attributes list
    # name (or ID if name is not available for the item)
    #  - if no name or ID is available for this type, name is set to gene_name + ' ' + human-readable type name
    #    (5UTR for KIR3DL3 --> KIR3DL3 5' UTR)
    "name": {"type": "text", "analyzer": "standard"},
    # contig:start-end - turn into searchable text
    "position": {"type": "text", "analyzer": "standard"},

    "type": {"type": "keyword", "analyzer": "standard"},  # feature type - gene/exon/transcript/5UTR/3UTR/...
    "genome": {"type": "keyword", "analyzer": "standard"},  # genome ID
    "strand": {"type": "keyword", "analyzer": "standard"},  # strand + or - TODO
}
