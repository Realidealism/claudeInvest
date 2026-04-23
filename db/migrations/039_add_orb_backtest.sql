-- ORB backtest results table.
--
-- One row per (trade_date, stock_id, strategy, entry_method) — each ORB
-- signal expands into 3 strategies × 2 entry models = 6 backtest rows.
--
-- strategy:
--   PT1_exit         exit at pt1_level; stop at reversed (opposite OR side)
--   PT2_exit         exit at pt2_level; stop at reversed
--   PT1_then_trail   after pt1 hit, stop moves to breakout_price (breakeven+);
--                    exit at pt2, trail stop, or EOD close
--
-- entry_method:
--   level            enter at or_high (U) / or_low (D)           — theoretical
--   bar_close        enter at the breakout 1-min bar's close     — realistic
--
-- pnl_pct_gross: raw return as a decimal (0.015 = +1.5%)
-- pnl_pct_net:   after a flat 0.006 (0.6%) round-trip cost

CREATE TABLE IF NOT EXISTS tw.orb_backtest_trades (
    trade_date     DATE         NOT NULL,
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    strategy       VARCHAR(16)  NOT NULL,
    entry_method   VARCHAR(12)  NOT NULL,
    direction      CHAR(1)      NOT NULL,
    entry_price    NUMERIC(12, 4) NOT NULL,
    entry_at       TIMESTAMPTZ,
    exit_price     NUMERIC(12, 4) NOT NULL,
    exit_at        TIMESTAMPTZ,
    exit_reason    VARCHAR(16)  NOT NULL,
    pnl_pct_gross  NUMERIC(10, 4),
    pnl_pct_net    NUMERIC(10, 4),
    duration_min   INT,
    PRIMARY KEY (trade_date, stock_id, strategy, entry_method)
);

CREATE INDEX IF NOT EXISTS idx_orb_bt_strategy
    ON tw.orb_backtest_trades (strategy, entry_method);

CREATE INDEX IF NOT EXISTS idx_orb_bt_date
    ON tw.orb_backtest_trades (trade_date);
