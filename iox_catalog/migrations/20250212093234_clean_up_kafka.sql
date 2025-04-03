ALTER TABLE namespace DROP COLUMN IF EXISTS topic_id;
ALTER TABLE namespace DROP COLUMN IF EXISTS query_pool_id;
ALTER TABLE parquet_file DROP COLUMN IF EXISTS shard_id;
ALTER TABLE partition DROP COLUMN IF EXISTS shard_id;
ALTER TABLE partition DROP COLUMN IF EXISTS persisted_sequence_number;
ALTER TABLE partition DROP COLUMN IF EXISTS sort_key;

DROP TABLE IF EXISTS tombstone;
DROP TABLE IF EXISTS sharding_rule_override;
DROP TABLE IF EXISTS shard;
DROP TABLE IF EXISTS topic;
DROP TABLE IF EXISTS query_pool;
