-- Daily cross-sectional median of Parkinson-233 volatility.
--
-- Consumed by the ScoreBoard's low-volatility cell (analysis/score.py
-- `_add_volatility_rules`) as the denominator of the stock's relative volatility:
--
--     z = -(log(park233(stock) / median_vol) - MU) / SIGMA
--
-- The cell is scored per stock in isolation (ScoreBoard.evaluate sees one stock at a
-- time, and daily_snapshot fans it across processes), so a cross-sectional rank is not
-- reachable from inside a cell.  Dividing by a same-day constant is: it leaves the
-- cross-sectional ORDER identical to a rank, which is all the ranking needs.  This table
-- is that constant, and it is loaded once per board exactly like tw.market_breadth.
--
-- Why a stored series rather than the TAIEX index's own volatility: log(stock_vol /
-- taiex_vol) drifts badly across years (yearly mean +1.24 in 2017 down to +0.51 in 2026,
-- i.e. 1.4 sigma), which would rot the hardcoded MU/SIGMA constants.  Against the
-- cross-sectional median the same quantity is flat (yearly mean -0.03 to -0.07).
--
-- Universe: active TWSE/TPEx STOCK rows -- the same universe analysis/_score_panel.py
-- scores, so the constants calibrated on the panel stay valid in production.
CREATE TABLE IF NOT EXISTS tw.market_vol (
    trade_date   DATE PRIMARY KEY,
    median_vol   DOUBLE PRECISION NOT NULL,
    n_stocks     INTEGER NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
