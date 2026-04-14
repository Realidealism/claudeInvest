-- Market breadth (排多排空比例) daily snapshot
CREATE TABLE IF NOT EXISTS tw.market_breadth (
    trade_date      DATE PRIMARY KEY,
    active_stocks   INT NOT NULL,       -- all stocks with data
    total_stocks    INT NOT NULL,       -- non-dead-fish stocks

    short_up        INT NOT NULL,       -- 短排多 count
    short_down      INT NOT NULL,       -- 短排空 count
    medium_up       INT NOT NULL,       -- 中排多 count
    medium_down     INT NOT NULL,       -- 中排空 count
    long_up         INT NOT NULL,       -- 長排多 count
    long_down       INT NOT NULL,       -- 長排空 count

    created_at      TIMESTAMPTZ DEFAULT NOW()
);
