-- Add 台指期大台 TXF night-session quote to the night-reference table.
--
-- TXF (TAIFEX Taiwan index futures, continuous front month) trades a night
-- session (15:00–05:00 TPE) and is already in TAIEX points, so its price is
-- a direct implied-open — no conversion needed. Stored alongside the SGX 富台
-- columns so one row carries both overnight references for the same day.
-- Note: TAIFEX is fully closed on TW holidays (no day or night session), so
-- these columns go stale during a 連假 while the SGX 富台 columns keep moving.
ALTER TABLE tw.ftse_taiwan
    ADD COLUMN IF NOT EXISTS txf_now         NUMERIC(12, 2),
    ADD COLUMN IF NOT EXISTS txf_pct_change  NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS txf_captured_at TIMESTAMPTZ;
