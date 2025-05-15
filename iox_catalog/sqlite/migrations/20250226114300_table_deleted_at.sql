ALTER TABLE table_name ADD COLUMN deleted_at INTEGER DEFAULT NULL;
CREATE INDEX table_deleted_at_idx ON table_name (deleted_at);
