-- Insider pledge / release EVENTS (內部人設質解質公告, MOPS STAMAK03_1).
--
-- Event-grained: one row per pledge-change filing (含解質股數與異動日期),
-- distinct from and complementary to tw.insider_holdings (which is a monthly
-- cumulative holdings snapshot with only 累積設質, no release events).
--
-- Raw 設質人身份別 / 姓名 are kept verbatim (no role aggregation) so the
-- feed can surface exactly who pledged/released and to whom (質權人).
-- ROC dates from MOPS (e.g. 113/11/05) are converted to AD at ingest time.
CREATE TABLE IF NOT EXISTS tw.insider_pledge_events (
    id                 SERIAL PRIMARY KEY,
    stock_id           TEXT NOT NULL,
    insider_role       TEXT,           -- 設質人身份別 (raw)
    insider_name       TEXT,           -- 設質人姓名
    change_date        DATE,           -- 質設異動發生日期
    pledged_shares     BIGINT,         -- 設質股數
    released_shares    BIGINT,         -- 解質股數
    cumulative_pledged BIGINT,         -- 累積質設股數
    pledgee_name       TEXT,           -- 質權人姓名
    remark             TEXT,           -- 備註
    report_date        DATE,           -- 申報日期
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, change_date, insider_name, pledged_shares,
            released_shares, cumulative_pledged, report_date)
);

CREATE INDEX IF NOT EXISTS idx_insider_pledge_events_date
    ON tw.insider_pledge_events (change_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_pledge_events_stock
    ON tw.insider_pledge_events (stock_id);
