-- Add a source field to the parquet file table, but do not attempt to backfill existing parquet
-- file records. This will only be used going forward to determine whether a Parquet file came from
-- a bulk ingest operation or through the regular ingester/compactor data flow.
--
-- Also do not add an index; this field will only be queried if manual cleanup is necessary and
-- should be scoped to namespace/table/date such that doing a sequential scan over the remaining
-- records isn't a problem.
ALTER TABLE IF EXISTS parquet_file
ADD COLUMN source smallint;
