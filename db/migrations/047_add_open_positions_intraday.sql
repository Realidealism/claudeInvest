-- Intraday (12:50) preview of unified-strategy open positions.
-- Mirrors tw.open_positions; same per-(snapshot_date, snapshot_time)
-- keying convention as tw.score_snapshot_intraday so multiple cuts per
-- day are supported in the future.

CREATE TABLE IF NOT EXISTS tw.open_positions_intraday (
    snapshot_date    DATE          NOT NULL,
    snapshot_time    TIMESTAMPTZ   NOT NULL,
    stock_id         VARCHAR(10)   NOT NULL REFERENCES tw.stocks(stock_id),
    side             VARCHAR(5)    NOT NULL,
    entry_date       DATE          NOT NULL,
    entry_price      NUMERIC(10, 2) NOT NULL,
    entry_tier       VARCHAR(16)   NOT NULL,
    current_close    NUMERIC(10, 2) NOT NULL,
    pnl_pct          NUMERIC(7, 3) NOT NULL,
    bars_held        INTEGER       NOT NULL,
    turnover         NUMERIC(20, 2),
    defense_price    NUMERIC(10, 2),
    defense_reason   VARCHAR(128),
    defense_date     DATE,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, snapshot_time, stock_id, side),
    CONSTRAINT open_pos_intraday_side_chk CHECK (side IN ('long', 'short')),
    CONSTRAINT open_pos_intraday_tier_chk CHECK (
        entry_tier IN ('pick', 'buy', 'sell_flee', 'touch', 'sell', 'buy_flee')
    )
);

CREATE INDEX IF NOT EXISTS idx_open_pos_intraday_date
    ON tw.open_positions_intraday (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_open_pos_intraday_date_side
    ON tw.open_positions_intraday (snapshot_date, side);
