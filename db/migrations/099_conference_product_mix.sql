-- A product mix an investor-conference deck discloses and the annual report does not.
--
-- 64 classified companies file one revenue line too coarse to split across the
-- nodes they occupy (evidence_kind = 'filing_too_coarse'), so weight_pct can
-- never be filled from tw.product_revenue for any of them. 592 decks carrying a
-- mix keyword were read; 161 have a page whose TEXT (not pdfplumber's table
-- objects -- those come back full of chart rectangles) carries rows of label and
-- figures, and 8 of those belong to a company whose filing is too coarse.
--
-- Of the 8, one is worth having: 3583 辛耘, whose annual report gives only
-- accounting captions (銷貨收入 / 勞務收入 / 其他營業收入) and whose deck splits
-- the business 代理 58 / 製造業 42. Its FY2025 figure checks out against the
-- filing exactly -- deck p.36 puts 代理 revenue at 6,647,828 against the report's
-- 營業收入淨額 11,371,368, which is 58.46%.
--
-- This is a separate table rather than more rows in tw.product_revenue because a
-- deck is not a filing and the two disagree in ways that would be invisible once
-- merged:
--
--   axis        a deck picks whatever axis flatters it. 6996 力領's whole mix is
--               車用 / 工控, an application split; 3346 麗清 breaks revenue down
--               by customer; 6643 M31 by region. R031 forbids building an
--               exposure from an application axis, so the axis is recorded as a
--               column and fill_weight reads only 'product' and 'business_unit'.
--               Leaving it to be remembered is how the rule gets broken.
--   period      the report says FY2025 and a deck says whatever quarter it was
--               given in. Both are kept, labelled as the deck printed them.
--   entity      1314 中石化's deck is consolidated and includes a subsidiary at
--               27%; its annual report table is the parent alone. Same company,
--               different denominators, and nothing in the numbers says so.
--
-- What a deck is NOT allowed to do is overwrite an annual-report figure. Nothing
-- here can: fill_weight reads this table only for rows whose evidence_kind is
-- 'investor_conf', which is a judgement recorded per exposure in
-- db/industry_data.py, not something this table can claim for itself.

CREATE TABLE IF NOT EXISTS tw.conference_product_mix (
    stock_id     VARCHAR NOT NULL REFERENCES tw.stocks(stock_id),
    conf_date    DATE NOT NULL,
    -- As printed on the slide ('2025', '2026/Q2'). Kept verbatim: a deck that
    -- writes 2026/Q2 may mean the quarter or the half, and guessing which would
    -- put an interpretation into the key.
    period_label TEXT NOT NULL,
    period_end   DATE NOT NULL,
    item         TEXT NOT NULL,
    pct          NUMERIC(7,4),
    revenue      BIGINT,
    axis         TEXT NOT NULL,
    deck_page    INT,
    source_file  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_id, period_label, item),
    CONSTRAINT cpm_axis CHECK (axis IN
        ('product', 'business_unit', 'application', 'customer', 'region')),
    CONSTRAINT cpm_has_number CHECK (pct IS NOT NULL OR revenue IS NOT NULL),
    CONSTRAINT cpm_period_before_conf CHECK (period_end <= conf_date)
);

CREATE INDEX IF NOT EXISTS conference_product_mix_usable_idx
    ON tw.conference_product_mix (stock_id, period_end DESC)
    WHERE axis IN ('product', 'business_unit') AND pct IS NOT NULL;

COMMENT ON TABLE tw.conference_product_mix IS
    'Revenue mixes read off investor-conference decks. Kept apart from '
    'tw.product_revenue because decks differ from filings in axis, period and '
    'reporting entity. Only product/business_unit rows can carry a weight.';
COMMENT ON COLUMN tw.conference_product_mix.axis IS
    'What the deck split revenue by. application/customer/region are recorded '
    'but never become an exposure weight (R031).';
