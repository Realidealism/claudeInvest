-- Daily stock liquidity classification.
--
-- tw.stock_liquidity_daily: end-of-day rollup from daily_update.py.
--   money_level / is_dead_fish come from analysis.money.calculate_money()
--   is_halted: volume=0 today AND active_recent_count >= 3 in last 10 days
--              (volume-based retroactive rule; SinoPac-based pre-market
--               detection lives in stock_halts_today below)
--   is_on_alert: any row in tw.stock_alerts for the same trade_date
--
-- tw.stock_halts_today: pre-market halt list written by pre_market_update.py
--   from SinoPac contracts where `reference == 0` and the stock is still
--   active (not delisted). Used by the intraday ORB pipeline at 09:30 to
--   exclude today's halted names from the watch universe before the
--   liquidity_daily row for today exists.

CREATE TABLE IF NOT EXISTS tw.stock_liquidity_daily (
    trade_date    DATE         NOT NULL,
    stock_id      VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    money_level   SMALLINT,                          -- 0-11, analysis.money
    is_dead_fish  BOOLEAN      NOT NULL DEFAULT FALSE,
    is_halted     BOOLEAN      NOT NULL DEFAULT FALSE,
    is_on_alert   BOOLEAN      NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_liquidity_date_tradable
    ON tw.stock_liquidity_daily (trade_date)
    WHERE is_dead_fish = FALSE AND is_halted = FALSE;


CREATE TABLE IF NOT EXISTS tw.stock_halts_today (
    trade_date   DATE         NOT NULL,
    stock_id     VARCHAR(10)  NOT NULL REFERENCES tw.stocks(stock_id),
    detected_at  TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (trade_date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_halts_today_date
    ON tw.stock_halts_today (trade_date);
