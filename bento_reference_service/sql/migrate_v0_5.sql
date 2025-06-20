-- Run these commands before migrating to v0.5.x

-- In the past, we missed some constraint updates. They should have been executed for version 0.2.0.

ALTER TABLE genome_contigs DROP CONSTRAINT IF EXISTS genome_contigs_md5_checksum_key;
ALTER TABLE genome_contigs DROP CONSTRAINT IF EXISTS genome_contigs_ga4gh_checksum_key;

-- Hacky version of ADD UNIQUE IF NOT EXISTS for md5_checksum
ALTER TABLE genome_contigs DROP CONSTRAINT IF EXISTS genome_contigs_genome_id_md5_checksum_key;
ALTER TABLE genome_contigs ADD UNIQUE (genome_id, md5_checksum);

-- Hacky version of ADD UNIQUE IF NOT EXISTS for ga4gh_checksum
ALTER TABLE genome_contigs DROP CONSTRAINT IF EXISTS genome_contigs_genome_id_ga4gh_checksum_key;
ALTER TABLE genome_contigs ADD UNIQUE (genome_id, ga4gh_checksum);
