CREATE INDEX IF NOT EXISTS parquet_file_created_at
ON parquet_file (created_at)
WHERE to_delete IS NULL;
