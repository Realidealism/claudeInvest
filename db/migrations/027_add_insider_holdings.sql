-- Director / supervisor / manager monthly shareholding (董監經理人持股餘額).
-- Source: TWSE OpenAPI t187ap11_L (上市) + t187ap11_P (公發 incl. TPEx/ESB) for current month,
--         MOPS ajax_stapap1 for historical backfill.
-- Data is per-person; this table stores aggregated per-company per-month.
--
-- 職稱 classification:
--   supervisor: contains 監察人
--   director:   contains 董事 (includes 董事長, 獨立董事, 董事之法人代表人)
--   manager:    contains 總經理/副總/經理/協理/財務主管/會計主管/稽核
--   skip:       持股 10% 以上股東 (not employees)

CREATE TABLE IF NOT EXISTS tw.insider_holdings (
    stock_id           VARCHAR(10) NOT NULL,
    year_month         CHAR(6)     NOT NULL,  -- YYYYMM (AD year)
    director_shares    BIGINT,
    director_pledged   BIGINT,
    supervisor_shares  BIGINT,
    supervisor_pledged BIGINT,
    manager_shares     BIGINT,
    manager_pledged    BIGINT,
    insider_shares     BIGINT,  -- sum of all three
    insider_pledged    BIGINT,
    updated_at         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (stock_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_insider_holdings_ym
    ON tw.insider_holdings (year_month);
