-- TAIFEX futures / options historical data (期貨/選擇權)
--
-- Four datasets, all sourced from TAIFEX public CSV download endpoints
-- (Big5-encoded). Backfilled from 2015-01 onward by backfill_taifex.py.
-- Per-strike option chains are intentionally not stored; option positioning
-- is captured at the institutional-aggregate level in inst_options.
--
--   futures_daily  : 期貨每日行情 (台指系列)        cht/3/futDataDown        (commodity_id=all, per-day)
--   inst_futures   : 三大法人期貨買賣/未平倉        cht/3/futContractsDateDown (per-year range)
--   inst_options   : 三大法人選擇權買賣/未平倉      cht/3/callsAndPutsDateDown (per-year range)
--   pc_ratio       : 臺指選擇權 Put/Call 比          cht/3/pcRatioDown          (per-month range)
--
-- Amount columns are in thousands of NTD (千元), matching the source CSV.
-- Prices/volumes that the source reports as "-" (no trade) are stored NULL.

-- 期貨每日行情 ------------------------------------------------------------
-- contract is the product code (TX, MTX, stock-future codes, ...); a single
-- product has multiple contract_month rows (incl. spreads like 202608/202609)
-- and two sessions (一般 / 盤後).
CREATE TABLE IF NOT EXISTS tw.taifex_futures_daily (
    trade_date      DATE        NOT NULL,
    contract        TEXT        NOT NULL,       -- 契約 (product code)
    contract_month  TEXT        NOT NULL,       -- 到期月份(週別), e.g. 202607 / 202606W4 / 202608/202609
    session         TEXT        NOT NULL,       -- 交易時段: 一般 / 盤後
    open_price      NUMERIC(14, 4),
    high_price      NUMERIC(14, 4),
    low_price       NUMERIC(14, 4),
    close_price     NUMERIC(14, 4),
    change          NUMERIC(14, 4),             -- 漲跌價
    change_pct      NUMERIC(8, 4),              -- 漲跌% (sign kept, % stripped)
    volume          BIGINT,                     -- 成交量
    settlement      NUMERIC(14, 4),             -- 結算價
    open_interest   BIGINT,                     -- 未沖銷契約數
    best_bid        NUMERIC(14, 4),             -- 最後最佳買價
    best_ask        NUMERIC(14, 4),             -- 最後最佳賣價
    hist_high       NUMERIC(14, 4),             -- 歷史最高價
    hist_low        NUMERIC(14, 4),             -- 歷史最低價
    halted          BOOLEAN,                    -- 是否因訊息面暫停交易
    spread_volume   BIGINT,                     -- 價差對單式委託成交量
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, contract, contract_month, session)
);
CREATE INDEX IF NOT EXISTS idx_taifex_fut_daily_date ON tw.taifex_futures_daily (trade_date);
CREATE INDEX IF NOT EXISTS idx_taifex_fut_daily_contract ON tw.taifex_futures_daily (contract, trade_date);

-- 三大法人 期貨 ----------------------------------------------------------
-- product is the Chinese product name (商品名稱) as published; investor is
-- 身份別 (自營商 / 投信 / 外資及陸資). All *_amount columns are 千元.
CREATE TABLE IF NOT EXISTS tw.taifex_inst_futures (
    trade_date      DATE NOT NULL,
    product         TEXT NOT NULL,              -- 商品名稱
    investor        TEXT NOT NULL,              -- 身份別
    long_volume     BIGINT,                     -- 多方交易口數
    long_amount     BIGINT,                     -- 多方交易契約金額(千元)
    short_volume    BIGINT,                     -- 空方交易口數
    short_amount    BIGINT,                     -- 空方交易契約金額(千元)
    net_volume      BIGINT,                     -- 多空交易口數淨額
    net_amount      BIGINT,                     -- 多空交易契約金額淨額(千元)
    long_oi         BIGINT,                     -- 多方未平倉口數
    long_oi_amount  BIGINT,                     -- 多方未平倉契約金額(千元)
    short_oi        BIGINT,                     -- 空方未平倉口數
    short_oi_amount BIGINT,                     -- 空方未平倉契約金額(千元)
    net_oi          BIGINT,                     -- 多空未平倉口數淨額
    net_oi_amount   BIGINT,                     -- 多空未平倉契約金額淨額(千元)
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, product, investor)
);
CREATE INDEX IF NOT EXISTS idx_taifex_inst_fut_date ON tw.taifex_inst_futures (trade_date);

-- 三大法人 選擇權 --------------------------------------------------------
-- call_put: C (CALL) / P (PUT). Volumes are split into buy/sell sides.
CREATE TABLE IF NOT EXISTS tw.taifex_inst_options (
    trade_date      DATE NOT NULL,
    product         TEXT NOT NULL,              -- 商品名稱
    call_put        CHAR(1) NOT NULL,           -- C / P
    investor        TEXT NOT NULL,              -- 身份別
    buy_volume      BIGINT,                     -- 買方交易口數
    buy_amount      BIGINT,                     -- 買方交易契約金額(千元)
    sell_volume     BIGINT,                     -- 賣方交易口數
    sell_amount     BIGINT,                     -- 賣方交易契約金額(千元)
    net_volume      BIGINT,                     -- 交易口數買賣淨額
    net_amount      BIGINT,                     -- 交易契約金額買賣淨額(千元)
    buy_oi          BIGINT,                     -- 買方未平倉口數
    buy_oi_amount   BIGINT,                     -- 買方未平倉契約金額(千元)
    sell_oi         BIGINT,                     -- 賣方未平倉口數
    sell_oi_amount  BIGINT,                     -- 賣方未平倉契約金額(千元)
    net_oi          BIGINT,                     -- 未平倉口數買賣淨額
    net_oi_amount   BIGINT,                     -- 未平倉契約金額買賣淨額(千元)
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, product, call_put, investor)
);
CREATE INDEX IF NOT EXISTS idx_taifex_inst_opt_date ON tw.taifex_inst_options (trade_date);

-- 臺指選擇權 Put/Call 比 -------------------------------------------------
CREATE TABLE IF NOT EXISTS tw.taifex_pc_ratio (
    trade_date      DATE PRIMARY KEY,
    put_volume      BIGINT,                     -- 賣權成交量
    call_volume     BIGINT,                     -- 買權成交量
    pc_volume_ratio NUMERIC(10, 2),             -- 買賣權成交量比率%
    put_oi          BIGINT,                     -- 賣權未平倉量
    call_oi         BIGINT,                     -- 買權未平倉量
    pc_oi_ratio     NUMERIC(10, 2),             -- 買賣權未平倉量比率%
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);
