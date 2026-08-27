-- Parent -> investee relationships parsed from the MOPS consolidated-financials
-- note "列入合併財務報表之子公司" (server-java/t164sb01, REPORT_ID=C).
-- Every row in that note is by definition consolidated, so `consolidated`
-- defaults TRUE; the column exists for rows sourced elsewhere later.
-- investee_id is filled only when the investee name resolves to a listed or
-- previously listed stock in tw.stocks; unmatched investees keep the raw name.

CREATE TABLE IF NOT EXISTS tw.company_affiliations (
    parent_id     VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    period        VARCHAR(7)  NOT NULL,
    investor_name TEXT        NOT NULL DEFAULT '',
    investee_name TEXT        NOT NULL,
    investee_id   VARCHAR(10),
    main_business TEXT,
    holding_pct   NUMERIC(9,4),
    consolidated  BOOLEAN     NOT NULL DEFAULT TRUE,
    source        TEXT        NOT NULL DEFAULT 't164sb01',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (parent_id, period, investor_name, investee_name)
);

CREATE INDEX IF NOT EXISTS idx_tw_affiliations_investee ON tw.company_affiliations (investee_id);
CREATE INDEX IF NOT EXISTS idx_tw_affiliations_period ON tw.company_affiliations (period);
