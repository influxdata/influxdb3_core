-- IOX_NO_TRANSACTION
-- Remove unused parquet_file_created_at index.
DROP INDEX CONCURRENTLY IF EXISTS parquet_file_created_at;