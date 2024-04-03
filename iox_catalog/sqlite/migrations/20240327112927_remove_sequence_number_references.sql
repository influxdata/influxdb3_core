-- Remove the columns referring to kafka sequence numbers
ALTER TABLE parquet_file DROP COLUMN max_sequence_number;
ALTER TABLE partition DROP COLUMN persisted_sequence_number;
