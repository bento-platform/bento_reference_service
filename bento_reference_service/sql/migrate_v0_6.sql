-- Migration (v0.6): widen genome_features.feature_type from VARCHAR(15) -> VARCHAR(31)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'genome_features'
          AND column_name = 'feature_type'
          AND data_type = 'character varying'
          AND character_maximum_length = 15
    ) THEN
        ALTER TABLE genome_features
        ALTER COLUMN feature_type TYPE VARCHAR(31);
    END IF;
END $$;
-- End migration (v0.6)
