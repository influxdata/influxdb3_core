-- Remove the columns referring to Kafka based information
ALTER TABLE IF EXISTS namespace DROP COLUMN IF EXISTS query_pool_id, DROP COLUMN IF EXISTS topic_id;
ALTER TABLE IF EXISTS parquet_file DROP COLUMN IF EXISTS shard_id;
ALTER TABLE IF EXISTS partition DROP COLUMN IF EXISTS shard_id;
ALTER TABLE IF EXISTS tombstone DROP COLUMN IF EXISTS shard_id;
-- Remove the now unreferenced, unused tables and sequencers
DROP TABLE IF EXISTS query_pool, shard, sharding_rule_override, topic;
DROP SEQUENCE IF EXISTS query_pool_id_seq, sequencer_id_seq, sharding_rule_override_id_seq, kafka_topic_id_seq;
