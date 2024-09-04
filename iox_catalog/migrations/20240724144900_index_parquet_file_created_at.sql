-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY IF NOT EXISTS parquet_file_created_at
ON parquet_file (created_at)
WHERE to_delete IS NULL;
