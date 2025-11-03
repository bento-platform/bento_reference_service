-- Migration (v0.6): widen genome_features.feature_type from VARCHAR(15) -> VARCHAR(31)
ALTER TABLE genome_features ALTER COLUMN feature_type TYPE VARCHAR(31);
-- End migration (v0.6)
