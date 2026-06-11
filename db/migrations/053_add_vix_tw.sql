-- TAIEX Options Volatility Index (TAIFEX TWVIX)
-- https://www.taifex.com.tw/cht/7/vixDaily3MNew
--
-- Daily closing value of the TAIEX options-implied volatility index,
-- published by TAIFEX. Source files live at
-- https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/{YYYYMM}new.txt
-- and cover the trailing ~3 calendar months. The scraper pulls the
-- recent monthly files on every run and upserts; older history accrues
-- as we keep running daily.
CREATE TABLE IF NOT EXISTS tw.vix_tw (
    trade_date   DATE PRIMARY KEY,
    close        NUMERIC(6, 2) NOT NULL,
    fetched_at   TIMESTAMPTZ DEFAULT NOW()
);
