-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- Recreate potentially invalid indices.
 
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS namespace_name_deleted_at_unique_idx;

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
CREATE UNIQUE INDEX CONCURRENTLY
  IF NOT EXISTS namespace_name_deleted_at_unique_idx
  ON namespace (name, deleted_at)
  WHERE deleted_at IS NOT NULL;

-- IOX_STEP_BOUNDARY

-- Recreate potentially invalid indices.

-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS namespace_name_active_unique_idx;

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
CREATE UNIQUE INDEX CONCURRENTLY
  IF NOT EXISTS namespace_name_active_unique_idx
  ON namespace (name)
  WHERE deleted_at IS NULL;

-- IOX_STEP_BOUNDARY

-- Drop the old name unique constraint once both partial indexes are in place. 
ALTER TABLE
  IF EXISTS namespace
  DROP CONSTRAINT IF EXISTS namespace_name_unique;
