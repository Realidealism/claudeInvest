-- Quarterly financial statements (季度財報三表) + capital changes
-- Data source: MOPS (公開資訊觀測站) XBRL / HTML reports
-- All monetary values are stored in thousands of NTD (千元)
-- period_type: 'Q' = single quarter, 'A' = annual (cumulative)
-- Q1 reports are published by 5/15, Q2 by 8/14, Q3 by 11/14, Q4 (annual) by 3/31

-- Income Statement (損益表)
CREATE TABLE IF NOT EXISTS tw.income_statements (
    stock_id                VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    year                    SMALLINT NOT NULL,
    quarter                 SMALLINT NOT NULL,          -- 1,2,3,4
    period_type             CHAR(1) NOT NULL DEFAULT 'Q', -- 'Q' single-quarter, 'A' cumulative
    -- Revenue & gross profit
    revenue                 BIGINT,                      -- 營業收入
    cost_of_revenue         BIGINT,                      -- 營業成本
    gross_profit            BIGINT,                      -- 營業毛利
    -- Operating expenses breakdown
    selling_expenses        BIGINT,                      -- 推銷費用
    admin_expenses          BIGINT,                      -- 管理費用
    rd_expenses             BIGINT,                      -- 研發費用
    operating_expenses      BIGINT,                      -- 營業費用合計
    operating_income        BIGINT,                      -- 營業利益
    -- Non-operating
    non_operating_income    BIGINT,                      -- 業外損益合計
    interest_income         BIGINT,                      -- 利息收入
    interest_expense        BIGINT,                      -- 利息費用
    -- Pretax, tax, net
    pretax_income           BIGINT,                      -- 稅前淨利
    tax_expense             BIGINT,                      -- 所得稅費用
    net_income              BIGINT,                      -- 本期淨利 (含少數股權)
    net_income_attributable BIGINT,                      -- 歸屬母公司淨利
    -- Per share
    eps                     NUMERIC(10, 2),              -- 基本 EPS
    diluted_eps             NUMERIC(10, 2),              -- 稀釋 EPS
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, year, quarter, period_type)
);

-- Balance Sheet (資產負債表)
CREATE TABLE IF NOT EXISTS tw.balance_sheets (
    stock_id                 VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    year                     SMALLINT NOT NULL,
    quarter                  SMALLINT NOT NULL,
    -- Current assets
    cash_and_equivalents     BIGINT,                     -- 現金及約當現金
    short_term_investments   BIGINT,                     -- 短期投資
    accounts_receivable      BIGINT,                     -- 應收帳款淨額
    inventory                BIGINT,                     -- 存貨
    other_current_assets     BIGINT,                     -- 其他流動資產
    current_assets           BIGINT,                     -- 流動資產合計
    -- Non-current assets
    long_term_investments    BIGINT,                     -- 長期投資
    ppe                      BIGINT,                     -- 不動產廠房及設備淨額
    intangible_assets        BIGINT,                     -- 無形資產
    other_assets             BIGINT,                     -- 其他資產
    total_assets             BIGINT,                     -- 資產總計
    -- Current liabilities
    short_term_debt          BIGINT,                     -- 短期借款
    accounts_payable         BIGINT,                     -- 應付帳款
    other_current_liab       BIGINT,                     -- 其他流動負債
    current_liabilities      BIGINT,                     -- 流動負債合計
    -- Non-current liabilities
    long_term_debt           BIGINT,                     -- 長期借款
    other_liabilities        BIGINT,                     -- 其他負債
    total_liabilities        BIGINT,                     -- 負債總計
    -- Equity
    common_stock             BIGINT,                     -- 股本
    capital_surplus          BIGINT,                     -- 資本公積
    retained_earnings        BIGINT,                     -- 保留盈餘
    treasury_stock           BIGINT,                     -- 庫藏股
    total_equity             BIGINT,                     -- 權益總計 (含少數股權)
    equity_attributable      BIGINT,                     -- 歸屬母公司權益
    minority_interest        BIGINT,                     -- 非控制權益
    -- Shares
    shares_outstanding       BIGINT,                     -- 流通在外股數 (股)
    book_value_per_share     NUMERIC(10, 2),             -- 每股淨值
    created_at               TIMESTAMPTZ DEFAULT NOW(),
    updated_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, year, quarter)
);

-- Cash Flow Statement (現金流量表) — always cumulative YTD in MOPS
CREATE TABLE IF NOT EXISTS tw.cash_flows (
    stock_id                VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    year                    SMALLINT NOT NULL,
    quarter                 SMALLINT NOT NULL,
    period_type             CHAR(1) NOT NULL DEFAULT 'Q', -- 'Q' single-quarter (derived), 'A' cumulative YTD
    -- Operating activities
    net_income_cf           BIGINT,                      -- 本期淨利 (現金流表起點)
    depreciation            BIGINT,                      -- 折舊
    amortization            BIGINT,                      -- 攤銷
    operating_cash_flow     BIGINT,                      -- 營業活動現金流
    -- Investing activities
    capex                   BIGINT,                      -- 資本支出 (PPE 購置, 負值)
    investing_cash_flow     BIGINT,                      -- 投資活動現金流
    -- Financing activities
    dividends_paid          BIGINT,                      -- 發放現金股利
    financing_cash_flow     BIGINT,                      -- 融資活動現金流
    -- Summary
    net_change_in_cash      BIGINT,                      -- 本期現金增減
    free_cash_flow          BIGINT,                      -- OCF + CAPEX (capex 為負數)
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, year, quarter, period_type)
);

-- Capital changes (股本異動) — used for EPS restatement in PE band chart
CREATE TABLE IF NOT EXISTS tw.capital_changes (
    id              SERIAL PRIMARY KEY,
    stock_id        VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    effective_date  DATE NOT NULL,
    event_type      VARCHAR(30) NOT NULL,    -- 'CASH_INCREASE','STOCK_DIVIDEND','REDUCTION','EMPLOYEE_BONUS','TREASURY_CANCEL'
    ratio           NUMERIC(10, 6),          -- e.g. 0.05 for 5% stock dividend
    shares_delta    BIGINT,                  -- change in shares outstanding (signed)
    shares_after    BIGINT,                  -- shares outstanding after the event
    note            TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_tw_is_period       ON tw.income_statements (year, quarter);
CREATE INDEX IF NOT EXISTS idx_tw_bs_period       ON tw.balance_sheets (year, quarter);
CREATE INDEX IF NOT EXISTS idx_tw_cf_period       ON tw.cash_flows (year, quarter);
CREATE INDEX IF NOT EXISTS idx_tw_cap_chg_stock   ON tw.capital_changes (stock_id, effective_date);
