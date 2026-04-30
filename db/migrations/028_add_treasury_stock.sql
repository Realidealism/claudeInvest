-- Treasury stock buyback programs (庫藏股買回).
-- Source: MOPS t35sc09 bulk summary by date range.
-- One row per buyback program (company × board_date × purpose).
-- Purpose: 1=轉讓員工, 2=維護公司信用及股東權益, 3=轉換(認股權/特別股/可轉債).

CREATE TABLE IF NOT EXISTS tw.treasury_stock (
    stock_id           VARCHAR(10) NOT NULL,
    board_date         DATE        NOT NULL,
    purpose            SMALLINT    NOT NULL,
    shares_outstanding BIGINT,
    shares_planned     BIGINT,
    price_min          NUMERIC(10,2),
    price_max          NUMERIC(10,2),
    period_start       DATE,
    period_end         DATE,
    completed          BOOLEAN,
    shares_bought      BIGINT,
    shares_transferred BIGINT,
    execution_rate     NUMERIC(6,2),
    total_cost         BIGINT,
    avg_price          NUMERIC(10,2),
    pct_outstanding    NUMERIC(6,2),
    note               TEXT,
    PRIMARY KEY (stock_id, board_date, purpose)
);

CREATE INDEX IF NOT EXISTS idx_treasury_stock_board_date
    ON tw.treasury_stock (board_date);
