-- Remove the columns referring to kafka sequence numbers
ALTER TABLE IF EXISTS parquet_file DROP COLUMN IF EXISTS max_sequence_number;
ALTER TABLE IF EXISTS partition DROP COLUMN IF EXISTS persisted_sequence_number;
