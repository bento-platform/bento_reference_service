CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS genomes (
    id VARCHAR(31) NOT NULL PRIMARY KEY,
    md5_checksum VARCHAR(32) NOT NULL UNIQUE,  -- Hexadecimal string representation of MD5 checksum bytes
    ga4gh_checksum VARCHAR(63) NOT NULL UNIQUE,  -- GA4GH/VRS/RefGet 2-formatted checksum: SQ.(truncated SHA12, B64)
    fasta_uri TEXT NOT NULL UNIQUE,  -- Can be a local file URI, an S3 URI, a DRS URI, or an HTTPS resource.
    fai_uri TEXT NOT NULL UNIQUE, -- Corresponding .fa.fai for the FASTA. See fasta_uri for what this can be.
    gff3_gz_uri TEXT UNIQUE,  -- Optional GFF3 annotation URI for the genome.
    gff3_gz_tbi_uri TEXT UNIQUE,  -- Tabix index for the optional GFF3 annotation file for the genome.
    taxon_id VARCHAR(31) NOT NULL,  -- e.g., NCBITaxon:9606
    taxon_label TEXT NOT NULL  -- e.g., Homo sapiens
);

-- Migration (v0.2.0): add genomes.gff3_uri and genomes.gff3_tbi_uri if they do not exist:
ALTER TABLE genomes
    ADD COLUMN IF NOT EXISTS gff3_gz_uri TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS gff3_gz_tbi_uri TEXT UNIQUE;
-- End migration (v0.2.0)

CREATE TABLE IF NOT EXISTS genome_aliases (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes ON DELETE CASCADE,
    alias VARCHAR(31) NOT NULL,
    naming_authority VARCHAR(63) NOT NULL,
    PRIMARY KEY (genome_id, alias)
);

CREATE TABLE IF NOT EXISTS genome_contigs (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes ON DELETE CASCADE,
    contig_name VARCHAR(31) NOT NULL,
    contig_length INTEGER NOT NULL,
    circular BOOLEAN NOT NULL DEFAULT FALSE,  -- Whether this sequence is circular, e.g., the mitochondrial genome
    -- Checksums: the two checksums given here are the ones recommended for RefGet v2;
    -- see http://samtools.github.io/hts-specs/refget.html#checksum-calculation
    -- The UNIQUE constraint on these two columns creates a B-tree index on each, so contigs can be queried by checksum.
    md5_checksum VARCHAR(32) NOT NULL,  -- Hexadecimal string representation of MD5 checksum bytes
    ga4gh_checksum VARCHAR(63) NOT NULL,  -- GA4GH/VRS/RefGet 2-formatted checksum: SQ.(truncated SHA12, B64)
    -- Contigs are unique only within the context of a particular reference genome:
    PRIMARY KEY (genome_id, contig_name),
    UNIQUE (genome_id, md5_checksum),
    UNIQUE (genome_id, ga4gh_checksum)
);
CREATE INDEX IF NOT EXISTS genome_contigs_md5_checksum_idx ON genome_contigs (md5_checksum);
CREATE INDEX IF NOT EXISTS genome_contigs_ga4gh_checksum_idx ON genome_contigs (ga4gh_checksum);

CREATE TABLE IF NOT EXISTS genome_contig_aliases (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes ON DELETE CASCADE,
    contig_name VARCHAR(63) NOT NULL,
    alias VARCHAR(63) NOT NULL,
    naming_authority VARCHAR(63) NOT NULL,
    FOREIGN KEY (genome_id, contig_name) REFERENCES genome_contigs ON DELETE CASCADE,
    PRIMARY KEY (genome_id, contig_name, alias)
);

-- Features are in GFF3 format, i.e., terms from the Sequence Ontology
-- See https://github.com/The-Sequence-Ontology/SO-Ontologies
CREATE TABLE IF NOT EXISTS genome_feature_types (
    type_id VARCHAR(63) NOT NULL PRIMARY KEY  -- Term ID from the Sequence Ontology
);

DO $$ BEGIN
    CREATE TYPE strand_type AS ENUM ('-', '+', '?', '.');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS genome_features (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes,
    -- Feature location information, on the genome:
    contig_name VARCHAR(63) NOT NULL,
    strand strand_type NOT NULL,
    -- Feature characteristics / attributes:
    --  - technically, there can be multiple rows in a GFF3 file with the same ID, for discontinuous features.
    --    however, let's not support this, since it becomes tricky and doesn't help us much for our use cases.
    feature_id VARCHAR(63) NOT NULL,  -- Feature ID from the GFF3 file, in the context of the genome
    feature_name TEXT NOT NULL,
    feature_type VARCHAR(15) NOT NULL REFERENCES genome_feature_types,
    source TEXT NOT NULL,
    -- Keys:
    PRIMARY KEY (genome_id, feature_id),
    FOREIGN KEY (genome_id, contig_name) REFERENCES genome_contigs
);
CREATE INDEX IF NOT EXISTS genome_features_feature_id_trgm_gin ON genome_features USING GIN (feature_id gin_trgm_ops);
CREATE INDEX IF NOT EXISTS genome_features_feature_name_trgm_gin ON genome_features USING GIN (feature_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS genome_feature_entries (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes,
    feature_id VARCHAR(63) NOT NULL,
    start_pos INTEGER NOT NULL, -- 1-based, inclusive
    end_pos INTEGER NOT NULL, -- 1-based, exclusive - if start_pos == end_pos then it's a 0-length feature
    position_text TEXT NOT NULL,  -- chr:start-end style searchable string - cached for indexing purposes
    score FLOAT,
    phase SMALLINT,
    -- Keys:
    FOREIGN KEY (genome_id, feature_id) REFERENCES genome_features
);
CREATE INDEX IF NOT EXISTS genome_feature_entries_genome_feature_idx ON genome_feature_entries (genome_id, feature_id);
CREATE INDEX IF NOT EXISTS genome_feature_entries_position_text_trgm_gin
    ON genome_feature_entries
    USING GIN (position_text gin_trgm_ops);

-- in GFF3 files, features can have one or multiple parents within the same annotation file
--  - facilitate this via a many-to-many table
CREATE TABLE IF NOT EXISTS genome_feature_parents (
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes,
    feature_id VARCHAR(63) NOT NULL,
    parent_id VARCHAR(63) NOT NULL,
    FOREIGN KEY (genome_id, feature_id) REFERENCES genome_features,
    FOREIGN KEY (genome_id, parent_id) REFERENCES genome_features
);
CREATE INDEX IF NOT EXISTS genome_feature_parents_genome_feature_idx ON genome_feature_parents (genome_id, feature_id);

-- attributes can also have multiple values, so we don't enforce uniqueness on (genome_id, feature_id, attr_tag)
-- these are non-Parent, non-ID attributes
CREATE TABLE IF NOT EXISTS genome_feature_attributes (
    annotation_id SERIAL PRIMARY KEY,
    genome_id VARCHAR(31) NOT NULL REFERENCES genomes,
    feature_id VARCHAR(63) NOT NULL,
    attr_tag VARCHAR(63) NOT NULL,
    attr_val TEXT NOT NULL,
    FOREIGN KEY (genome_id, feature_id) REFERENCES genome_features
);
CREATE INDEX IF NOT EXISTS genome_feature_attributes_genome_feature_idx
    ON genome_feature_parents (genome_id, feature_id);
CREATE INDEX IF NOT EXISTS genome_feature_attributes_attr_idx
    ON genome_feature_attributes (genome_id, feature_id, attr_tag);
