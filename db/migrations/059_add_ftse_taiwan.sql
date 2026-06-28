-- 富台指數 → 理論 TAIEX (SGX FTSE Taiwan Index Futures)
--
-- Holiday reference: when the TW cash market is closed but SGX is open, the
-- front-month FTSE Taiwan future keeps trading. We convert its % move
-- since the last TW close back onto TAIEX:
--
--   theoretical_taiex = taiex_ref_close * (ftse_now / ftse_base)
--
-- ftse_base is the future's close on the last TW cash trading day, carried
-- forward across a holiday/weekend stretch. Source: cnyes quote API
-- (continuous symbol GF:TWNCON:FUTURES), which unlike Yahoo includes the
-- night session. One row per run/calendar day.
CREATE TABLE IF NOT EXISTS tw.ftse_taiwan (
    trade_date         DATE PRIMARY KEY,
    front_contract     TEXT,
    ftse_now           NUMERIC(12, 2),
    ftse_base          NUMERIC(12, 2),
    pct_change         NUMERIC(10, 6),
    taiex_ref_date     DATE,
    taiex_ref_close    NUMERIC(12, 2),
    theoretical_taiex  NUMERIC(12, 2),
    captured_at        TIMESTAMPTZ,
    fetched_at         TIMESTAMPTZ DEFAULT NOW()
);
