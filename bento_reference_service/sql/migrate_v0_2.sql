-- Run these commands before migrating to v0.2.x

DROP TABLE genome_feature_annotations CASCADE;
DROP TABLE genome_feature_features CASCADE;
DROP TABLE IF EXISTS genome_feature_type_synonyms;  -- from v0.1, now unused

DROP TYPE strand_type CASCADE;
