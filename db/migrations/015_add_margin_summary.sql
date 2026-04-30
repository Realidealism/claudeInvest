-- Market-wide margin trading summary (信用交易統計)
-- Source: TWSE MI_MARGN tables[0]
-- Units: margin/short counts in lots (張), values in thousands NTD (仟元)
CREATE TABLE IF NOT EXISTS tw.margin_summary (
    trade_date           DATE PRIMARY KEY,

    -- 融資 (交易單位=張)
    margin_buy           INT,
    margin_sell          INT,
    margin_repay         INT,
    margin_prev_balance  INT,
    margin_balance       INT,

    -- 融券 (交易單位=張)
    short_buy            INT,
    short_sell           INT,
    short_repay          INT,
    short_prev_balance   INT,
    short_balance        INT,

    -- 融資金額 (仟元)
    margin_buy_value     BIGINT,
    margin_sell_value    BIGINT,
    margin_repay_value   BIGINT,
    margin_prev_value    BIGINT,
    margin_balance_value BIGINT
);
