-- Opening Range Breakout (ORB) — intraday pattern tracking.
--
-- intraday_opening_range: the 09:00–09:30 high/low snapshot, frozen at 09:30
-- by intraday.orb.freeze_opening_range(). Source is tw.intraday_quotes'
-- high_price / low_price which have been accumulating since the session open.
-- One row per (trade_date, stock_id). Re-runs overwrite via ON CONFLICT so
-- the freeze is idempotent within the session.
--
-- intraday_orb_signals: event log for the 3 stages of an ORB pattern.
-- At most one row per (trade_date, stock_id):
--   breakout_at       first crossing of or_high / or_low after 09:30
--   pt1_hit_at        first crossing of the 0.5×range extension level
--   pt2_hit_at        first crossing of the 1.0×range extension level
--   reversed_at       price crossed back through the OPPOSITE side of OR
--                     (fakeout) after initial breakout
-- Empty timestamps = stage not yet hit.

CREATE TABLE IF NOT EXISTS tw.intraday_opening_range (
    trade_date      DATE         NOT NULL,
    stock_id        VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    or_high         NUMERIC(12, 4) NOT NULL,
    or_low          NUMERIC(12, 4) NOT NULL,
    or_range        NUMERIC(12, 4) NOT NULL,
    established_at  TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_orb_range_date
    ON tw.intraday_opening_range (trade_date);


CREATE TABLE IF NOT EXISTS tw.intraday_orb_signals (
    trade_date      DATE         NOT NULL,
    stock_id        VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    direction       CHAR(1)      NOT NULL,             -- 'U' or 'D'
    breakout_price  NUMERIC(12, 4) NOT NULL,
    breakout_at     TIMESTAMPTZ  NOT NULL,
    pt1_hit_at      TIMESTAMPTZ,
    pt2_hit_at      TIMESTAMPTZ,
    reversed_at     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_orb_signals_date
    ON tw.intraday_orb_signals (trade_date);
