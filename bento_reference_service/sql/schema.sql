CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS genomes (
    id VARCHAR(31) NOT NULL PRIMARY KEY,
    md5_hex VARCHAR(32) NOT NULL UNIQUE,  -- Hexadecimal string representation of MD5 checksum bytes
    ga4gh_checksum VARCHAR(63) NOT NULL UNIQUE,
    fasta_uri TEXT NOT NULL UNIQUE,  -- Can be a local file URI, an S3 URI, a DRS URI, or an HTTPS resource.
    fai_uri TEXT NOT NULL UNIQUE -- Corresponding .fa.fai for the FASTA. See fasta_uri for what this can be.
);

CREATE TABLE IF NOT EXISTS genome_aliases (
    genome_id VARCHAR(31) NOT NULL FOREIGN KEY REFERENCES genomes,
    alias VARCHAR(31) NOT NULL,
    naming_authority VARCHAR(63) NOT NULL,
    PRIMARY KEY (genome_id, alias)
);

CREATE TABLE IF NOT EXISTS genome_contigs (
    genome_id VARCHAR(31) NOT NULL FOREIGN KEY REFERENCES genomes,
    contig_name VARCHAR(31) NOT NULL,
    contig_length INTEGER NOT NULL,
    circular BOOLEAN NOT NULL DEFAULT FALSE,  -- Whether this sequence is circular, e.g., the mitochondrial genome
    md5_hex VARCHAR(32) NOT NULL UNIQUE,  -- Hexadecimal string representation of MD5 checksum bytes
    ga4gh_checksum VARCHAR(63) NOT NULL UNIQUE,
    PRIMARY KEY (genome_id, contig_name)
);

CREATE TABLE IF NOT EXISTS genome_contig_aliases (
    genome_id VARCHAR(31) NOT NULL FOREIGN KEY REFERENCES genomes,
    contig_name VARCHAR(63) NOT NULL,
    alias VARCHAR(63) NOT NULL,
    naming_authority VARCHAR(63) NOT NULL,
    FOREIGN KEY (genome_id, contig_name) REFERENCES genome_contigs,
    PRIMARY KEY (genome_id, contig_name, alias)
);

-- Features are in GFF3 format, i.e., terms from the Sequence Ontology
-- See https://github.com/The-Sequence-Ontology/SO-Ontologies
CREATE TABLE IF NOT EXISTS genome_feature_types (
    type_id VARCHAR(63) NOT NULL PRIMARY KEY,  -- Term ID from the Sequence Ontology
);
CREATE TABLE IF NOT EXISTS genome_feature_type_synonyms (
    type_id VARCHAR(63) NOT NULL FOREIGN KEY REFERENCES genome_feature_types,
    synonym VARCHAR(63) NOT NULL UNIQUE,
    PRIMARY KEY (type_id, synonym)
);

CREATE TABLE IF NOT EXISTS genome_features (
    genome_id VARCHAR(31) NOT NULL FOREIGN KEY REFERENCES genomes,
    feature_id VARCHAR(63) NOT NULL,
    feature_name TEXT NOT NULL,
    position_text TEXT NOT NULL,  -- chr:start-end style searchable string
    feature_type VARCHAR(15) NOT NULL FOREIGN KEY REFERENCES genome_feature_types,
    strand_pos BOOLEAN,  -- NULL: strand not relevant; TRUE: (+); FALSE: (-)
    -- TODO: add position and contig foreign key rather than genome_id
    PRIMARY KEY (genome_id, feature_id)
);
CREATE INDEX IF NOT EXISTS genome_features_feature_name_trgm_gin ON genome_features USING (feature_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS genome_features_position_text_trgm_gin ON genome_features USING (position_text gin_trgm_ops);
