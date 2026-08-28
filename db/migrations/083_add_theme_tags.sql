-- Theme tags: the second, narrative dimension of the classification.
--
-- tw.themes answers "what does this company actually make", and every row in
-- it can be checked against the annual report's product-line mix. Market
-- themes cannot: AI ASIC, HBM, CoWoS, 先進製程, 矽光子 have no line item in any
-- filing, so admitting them into tw.themes would mean dropping the ~20%
-- revenue rule that makes that table worth trusting — and once dropped, the
-- whole table degrades into an unauditable news list.
--
-- So they live here instead, under a different and openly weaker contract:
--   level    'core'        the company puts real resources into it
--            'peripheral'  genuine exposure, but not where the money is
--            'speculative' market attribution only, no company confirmation
--   evidence one sentence saying why, always with a source. Required — the
--            loader refuses a row without it. A tag whose members cannot be
--            explained a year later is exactly the failure mode this table
--            exists to avoid.
--
-- A company can carry many tags, and tags cut across supply chains on purpose
-- (HBM reaches DRAM, 先進封裝, 載板, 設備 and 材料 at once). The two dimensions
-- are independent: nothing here references tw.themes.

CREATE TABLE IF NOT EXISTS tw.theme_tags (
    tag_id      SERIAL PRIMARY KEY,
    category    VARCHAR(30),                    -- grouping only, e.g. 'AI'; NULL = top level
    name        VARCHAR(50)  NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Same path-uniqueness idea as tw.themes (see 074): a tag name only has to be
-- unique inside its category, and NULL categories must still collide.
CREATE UNIQUE INDEX IF NOT EXISTS theme_tags_path_key
    ON tw.theme_tags (category, name) NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS tw.stock_theme_tags (
    stock_id      VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    tag_id        INTEGER     NOT NULL REFERENCES tw.theme_tags(tag_id),
    level         TEXT        NOT NULL
                  CHECK (level IN ('core', 'peripheral', 'speculative')),
    evidence      TEXT        NOT NULL CHECK (evidence <> ''),
    evidence_kind TEXT,       -- 'annual_report_prose' | 'investor_conf' | 'company_site' | 'news'
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_tw_stock_theme_tags_tag
    ON tw.stock_theme_tags (tag_id);
