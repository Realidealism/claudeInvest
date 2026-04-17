-- Stock alerts: attention (注意) and disposal (處置) announcements.
-- Sources: TWSE announcement/notice + punish, TPEx bulletin/attention + disposal.

CREATE TABLE IF NOT EXISTS tw.stock_alerts (
    stock_id      VARCHAR(10)  NOT NULL,
    alert_date    DATE         NOT NULL,
    alert_type    VARCHAR(10)  NOT NULL,  -- 'attention' or 'disposal'
    market        VARCHAR(10)  NOT NULL,  -- 'TWSE' or 'TPEx'
    cumulative    SMALLINT,
    reason        TEXT,
    period_start  DATE,
    period_end    DATE,
    measure       TEXT,
    close_price   NUMERIC(10,2),
    pe_ratio      NUMERIC(10,2),
    PRIMARY KEY (stock_id, alert_date, alert_type, market)
);

CREATE INDEX IF NOT EXISTS idx_stock_alerts_date
    ON tw.stock_alerts (alert_date);

CREATE INDEX IF NOT EXISTS idx_stock_alerts_type_date
    ON tw.stock_alerts (alert_type, alert_date);
