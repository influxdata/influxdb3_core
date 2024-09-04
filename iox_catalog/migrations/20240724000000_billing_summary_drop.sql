DROP TRIGGER IF EXISTS decrement_summary ON parquet_file;
DROP FUNCTION IF EXISTS maybe_decrement_billing_summary;
DROP TRIGGER IF EXISTS update_billing ON parquet_file;
DROP FUNCTION IF EXISTS increment_billing_summary;
DROP TABLE IF EXISTS billing_summary;
