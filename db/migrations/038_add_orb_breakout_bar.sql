-- Store the OHLC of the 1-minute bar that FIRST broke the opening range so
-- the backtest can compare two entry models: entering at the OR level
-- (or_high / or_low) vs. entering at the breakout bar's close (more
-- realistic — always ≥ the level for U breakouts, ≤ for D).
--
-- Nullable because early rows (from the initial backfill) don't carry
-- these; backfill_orb.py populates them on the next run via ON CONFLICT
-- DO UPDATE.

ALTER TABLE tw.intraday_orb_signals
    ADD COLUMN IF NOT EXISTS breakout_bar_open  NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS breakout_bar_high  NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS breakout_bar_low   NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS breakout_bar_close NUMERIC(12, 4);
