-- Daily snapshot of currently-open positions under the unified strategy.
-- Written by analysis/position_snapshot.py at the end of daily_update.
-- A position is "open" when the engine's force-close at end-of-history
-- emits a Trade with exit_reason = '回測結束' on the latest bar; we extract
-- entry info + the latest defense event into one row per (date, stock, side).

CREATE TABLE IF NOT EXISTS tw.open_positions (
    snapshot_date    DATE          NOT NULL,
    stock_id         VARCHAR(10)   NOT NULL REFERENCES tw.stocks(stock_id),
    side             VARCHAR(5)    NOT NULL,
    entry_date       DATE          NOT NULL,
    entry_price      NUMERIC(10, 2) NOT NULL,
    entry_tier       VARCHAR(16)   NOT NULL,
    current_close    NUMERIC(10, 2) NOT NULL,
    pnl_pct          NUMERIC(7, 3) NOT NULL,    -- mark-to-market pct (signed; short reversed)
    bars_held        INTEGER       NOT NULL,
    turnover         NUMERIC(20, 2),            -- snapshot-day turnover, for ranking
    defense_price    NUMERIC(10, 2),            -- latest defense level (NaN-ok → NULL)
    defense_reason   VARCHAR(128),              -- e.g. "保底" / "站上21日均線[pick]"
    defense_date     DATE,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, stock_id, side),
    CONSTRAINT open_pos_side_chk CHECK (side IN ('long', 'short')),
    CONSTRAINT open_pos_tier_chk CHECK (
        entry_tier IN ('pick', 'buy', 'sell_flee', 'touch', 'sell', 'buy_flee')
    )
);

CREATE INDEX IF NOT EXISTS idx_open_pos_date      ON tw.open_positions (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_open_pos_date_side ON tw.open_positions (snapshot_date, side);
