-- Function that updates the new_file_at field in the partition table when the update_partition trigger is fired
-- The field new_file_at signals when the last new file was added to the partition for compaction.
-- This only updates for L0 files (new from ingestor) not L1 L2 (new from compactor).
-- Historically new_file_at was updated for all new files, so a partition that has gone cold will continue to be
-- worked on even if the compactor times out several times (making progress each time but not enough to finish).
-- With priority scheduling we'll immediately recompact the partition if we didn't finish it last time (so the 
-- original motivation for updating on every new file is gone), and if cold compaction doesn't finish within
-- one timeout, we'd rather the compaction not make it look hot again, so it's immediately eligible for more cold
-- compaction.
CREATE OR REPLACE FUNCTION update_partition_on_new_file_at()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS $$
BEGIN
    IF NEW.compaction_level < 1 THEN
        UPDATE partition SET new_file_at = NEW.created_at WHERE id = NEW.partition_id;    
    END IF;

    RETURN NEW;
END;
$$;