-- TDCC weekly shareholder distribution (集保戶股權分散表).
-- Snapshot taken every Friday; published Saturday or early the following week.
-- 17 tiers: 1-15 are real holding ranges (1-999 shares through 1,000,001+ shares),
-- tier 16 is 差異數調整 (reconciliation adjustment), tier 17 is 合計 (total).

CREATE TABLE IF NOT EXISTS tw.shareholder_distribution (
    stock_id    VARCHAR(10) NOT NULL,
    data_date   DATE        NOT NULL,
    t1_holders  BIGINT, t1_shares  BIGINT, t1_pct  NUMERIC(6,2),
    t2_holders  BIGINT, t2_shares  BIGINT, t2_pct  NUMERIC(6,2),
    t3_holders  BIGINT, t3_shares  BIGINT, t3_pct  NUMERIC(6,2),
    t4_holders  BIGINT, t4_shares  BIGINT, t4_pct  NUMERIC(6,2),
    t5_holders  BIGINT, t5_shares  BIGINT, t5_pct  NUMERIC(6,2),
    t6_holders  BIGINT, t6_shares  BIGINT, t6_pct  NUMERIC(6,2),
    t7_holders  BIGINT, t7_shares  BIGINT, t7_pct  NUMERIC(6,2),
    t8_holders  BIGINT, t8_shares  BIGINT, t8_pct  NUMERIC(6,2),
    t9_holders  BIGINT, t9_shares  BIGINT, t9_pct  NUMERIC(6,2),
    t10_holders BIGINT, t10_shares BIGINT, t10_pct NUMERIC(6,2),
    t11_holders BIGINT, t11_shares BIGINT, t11_pct NUMERIC(6,2),
    t12_holders BIGINT, t12_shares BIGINT, t12_pct NUMERIC(6,2),
    t13_holders BIGINT, t13_shares BIGINT, t13_pct NUMERIC(6,2),
    t14_holders BIGINT, t14_shares BIGINT, t14_pct NUMERIC(6,2),
    t15_holders BIGINT, t15_shares BIGINT, t15_pct NUMERIC(6,2),
    t16_holders BIGINT, t16_shares BIGINT, t16_pct NUMERIC(6,2),
    t17_holders BIGINT, t17_shares BIGINT, t17_pct NUMERIC(6,2),
    PRIMARY KEY (stock_id, data_date)
);

CREATE INDEX IF NOT EXISTS idx_shareholder_dist_date
    ON tw.shareholder_distribution (data_date);
