-- Daily snapshot of hermit_stock screener output (top picks).
-- Written by hermit_stock/scripts/daily_check.py at the end of daily_update,
-- whenever any new financial / monthly / price data was scraped that day.
-- Used for day-over-day diff (NEW entrants, EXITs, rank shifts).

CREATE TABLE IF NOT EXISTS tw.hermit_screen_snapshot (
    snapshot_date  DATE         NOT NULL,
    rank           INTEGER      NOT NULL,            -- 1 = best
    stock_id       VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    score          SMALLINT     NOT NULL,            -- 0..8 (raw 8-rule)
    grade          CHAR(1)      NOT NULL,            -- A/B/C/D
    -- Gate pass/fail flags (each rule)
    f1_pass        BOOLEAN,
    f2_pass        BOOLEAN,
    f3_pass        BOOLEAN,
    f4_pass        BOOLEAN,
    f5_pass        BOOLEAN,
    f6_pass        BOOLEAN,
    f7_pass        BOOLEAN,
    f8_pass        BOOLEAN,
    -- Valuation snapshot (Stage-2 outputs from screener)
    val_method     VARCHAR(4),                       -- PE / PB / PS / NULL
    val_multiple   NUMERIC(10, 2),
    val_band       VARCHAR(20),                      -- "−1σ ~ mean", etc.
    val_upside_pct NUMERIC(8, 2),                    -- (target - close) / close * 100
    val_decision   VARCHAR(8),                       -- BUY / HOLD / SELL
    -- Diff flags vs previous snapshot
    is_new         BOOLEAN      NOT NULL DEFAULT FALSE,
    prev_rank      INTEGER,                          -- NULL if new entrant
    rank_delta     INTEGER,                          -- +up / -down (NULL if new)
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_hermit_snap_date ON tw.hermit_screen_snapshot (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_hermit_snap_stock ON tw.hermit_screen_snapshot (stock_id);
