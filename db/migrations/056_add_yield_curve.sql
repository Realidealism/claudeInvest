-- US Treasury yield curve daily series (FRED)
--
-- DGS10  : 10-Year  Treasury Constant Maturity Rate
-- DGS2   : 2-Year   Treasury Constant Maturity Rate
-- DGS3MO : 3-Month  Treasury Constant Maturity Rate
--
-- Spreads (10Y-2Y, 10Y-3M) are computed in export/generate.py at read time
-- rather than persisted, so any future revision to a raw FRED point
-- propagates without a separate backfill of the derived columns.
--
-- FRED publishes ~one calendar day late (e.g. Monday's number lands
-- Tuesday after 17:00 US/Eastern). yield_update.py runs at 09:35 TPE on
-- weekdays — by that point the previous US trading day's value is
-- already available.
CREATE TABLE IF NOT EXISTS tw.yield_curve (
    trade_date   DATE PRIMARY KEY,
    dgs10        NUMERIC(6, 3),
    dgs2         NUMERIC(6, 3),
    dgs3mo       NUMERIC(6, 3),
    fetched_at   TIMESTAMPTZ DEFAULT NOW()
);
