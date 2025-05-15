-- We cannot drop a unique constraint included in the create table definition
-- so must create a new table without the unique constraint to remove the old
-- namespace name constraint.
CREATE TABLE namespace_new
(
    id                    INTEGER
        constraint namespace_pkey
        primary key autoincrement,
    name                  varchar               not null,
    max_tables            integer default 10000 not null,
    max_columns_per_table integer default 200   not null,
    retention_period_ns   numeric,
    deleted_at            numeric DEFAULT NULL,
    partition_template    TEXT,
    router_version        BIGINT NOT NULL DEFAULT 0,
    generation            INTEGER NOT NULL DEFAULT 0
);

-- Copy the data from the old table to the new table
INSERT INTO namespace_new 
SELECT * FROM namespace;

-- Create the partial indices
CREATE UNIQUE INDEX IF NOT EXISTS namespace_name_deleted_at_unique_idx
  ON namespace_new (name, deleted_at)
  WHERE deleted_at IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS namespace_name_active_unique_idx
  ON namespace_new (name)
  WHERE deleted_at IS NULL;

-- Drop the old table
DROP TABLE namespace;

-- Rename the new table to the original name
ALTER TABLE namespace_new RENAME TO namespace;

-- Recreate the old indexes that we want to keep
CREATE INDEX namespace_deleted_at_idx ON namespace (deleted_at);
