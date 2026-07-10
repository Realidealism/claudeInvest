-- Track the SGX 富台 (TWN0000) latest daily-K bar date for 未開盤 detection.
--
-- The FtseTxfUpdate job compares the newest 富台 daily K-line date against this
-- stored value: if it has not advanced, SGX did not open a new trading day
-- (holiday) and the 富台 leg is marked 未開盤 (columns left null) rather than
-- republishing a stale price. The row itself is kept latest-only (older rows
-- are pruned on write), so this column always holds the last seen bar date.
ALTER TABLE tw.ftse_taiwan
    ADD COLUMN IF NOT EXISTS ftse_bar_date DATE;
