-- Insider pre-declared share transfers (內部人持股轉讓事前申報, MOPS ajax_t56sb21)
-- and company private placements (私募有價證券, MOPS ajax_t116sb01).
--
-- Both feed a passive "insider selling / dilution" avoid-overlay. The tradeable
-- signal is transfer_method='洽特定人' (insider block-sale to a specific buyer,
-- validated ~-2.5pp/20d in liquid names); company common-stock private placements
-- are a weaker small-cap avoid flag. Event dates are the first public dates:
-- 申報日期 for transfers, 董事會決議日 for placements. ROC dates -> AD at ingest.

CREATE TABLE IF NOT EXISTS tw.insider_share_transfers (
    id              SERIAL PRIMARY KEY,
    stock_id        TEXT NOT NULL,
    report_date     DATE NOT NULL,       -- 申報日期 (public announcement)
    insider_role    TEXT,                -- 申報人身分 (raw)
    insider_name    TEXT,                -- 姓名
    transfer_method TEXT,                -- 轉讓方式 (一般交易/信託/贈與/洽特定人...)
    transfer_shares BIGINT DEFAULT 0,    -- 轉讓股數
    planned_shares  BIGINT DEFAULT 0,    -- 預定轉讓總股數
    transfer_period TEXT,                -- 有效轉讓期間 (預定起~迄)
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, report_date, insider_name, transfer_method,
            transfer_shares, planned_shares)
);

CREATE INDEX IF NOT EXISTS idx_insider_transfers_date
    ON tw.insider_share_transfers (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_transfers_method
    ON tw.insider_share_transfers (transfer_method);
CREATE INDEX IF NOT EXISTS idx_insider_transfers_stock
    ON tw.insider_share_transfers (stock_id);

CREATE TABLE IF NOT EXISTS tw.private_placements (
    id            SERIAL PRIMARY KEY,
    stock_id      TEXT NOT NULL,
    decide_date   DATE NOT NULL,         -- 董事會決議日 (event date)
    security_kind TEXT,                  -- 證券種類 (普通股/轉換公司債/特別股...)
    market        TEXT,                  -- sii / otc
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, decide_date, security_kind)
);

CREATE INDEX IF NOT EXISTS idx_private_placements_date
    ON tw.private_placements (decide_date DESC);
CREATE INDEX IF NOT EXISTS idx_private_placements_stock
    ON tw.private_placements (stock_id);
