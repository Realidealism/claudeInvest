-- tw.position_revisions gains a fifth cause, exited_hidden: a real exit that
-- no row of the snapshot can show, carried with its exit_date like
-- exited_unseen:
--   * an exit on the stock's last bar followed on that bar by a new entry on
--     the same side -- tw.open_positions keeps one row per stock and side a
--     night, and it goes to the new position;
--   * a gap exit (異常跳價), which the engine dates back to the bar before the
--     gap -- the previous snapshot night, which showed the position held.
-- Until now both were recorded as redated or gone (analysis.daily_snapshot).
--
-- 117 left its CHECKs unnamed; the two that name exited_unseen are replaced
-- by named ones. Rerunnable: the old ones are dropped by definition, the new
-- ones added only when missing.
DO $$
DECLARE c record;
BEGIN
    FOR c IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'tw.position_revisions'::regclass AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%exited_unseen%'
          AND conname NOT IN ('position_revisions_cause_ck', 'position_revisions_exit_date_ck')
    LOOP
        EXECUTE format('ALTER TABLE tw.position_revisions DROP CONSTRAINT %I', c.conname);
    END LOOP;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'tw.position_revisions'::regclass
                     AND conname = 'position_revisions_cause_ck') THEN
        ALTER TABLE tw.position_revisions ADD CONSTRAINT position_revisions_cause_ck
            CHECK (cause IN ('redated', 'gone', 'not_run', 'exited_unseen', 'exited_hidden'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'tw.position_revisions'::regclass
                     AND conname = 'position_revisions_exit_date_ck') THEN
        ALTER TABLE tw.position_revisions ADD CONSTRAINT position_revisions_exit_date_ck
            CHECK ((cause IN ('exited_unseen', 'exited_hidden')) = (exit_date IS NOT NULL));
    END IF;
END $$;
