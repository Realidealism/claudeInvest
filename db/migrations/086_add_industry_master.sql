-- Industry classification master, rebuilt on the V3.7 schema.
--
-- What this replaces: tw.themes / tw.stock_themes held 69 nodes covering two
-- industries (半導體, 電子基板) and gave a company exactly one membership per
-- node, with no notion of which of its businesses was the main one and no
-- record of when the fact became true. That was enough to describe a supply
-- chain and not enough to classify a market.
--
-- What changes here:
--   * the tree is full-market and lives in tw.industry_tree as real nodes with
--     stable codes, so a membership references a node instead of repeating its
--     name as free text (a typo used to create a silent phantom category);
--   * a company carries MANY exposures with a role — exactly one Core, any
--     number of Major/Minor/Emerging — so 台達電 can be both power and thermal
--     without either being pretended to be its whole business;
--   * every row is dated. effective_from/to say when the fact held, known_date
--     says when we could have known it. Backtests must filter on known_date;
--     that is the whole reason the column exists (R021).
--
-- What is deliberately kept from the old tables: evidence is still NOT NULL and
-- still carries evidence_kind. The V3.7 draft graded rows with confidence A~D
-- alone, which is an opinion about a row rather than a reason for it. Both are
-- here: confidence is the grade, evidence is the quote it was given for.
--
-- Rules referenced above are R001-R021, loaded into tw.classification_rules.

-- ---------- enumerated types ----------
DO $$ BEGIN
    CREATE TYPE tw.exposure_role AS ENUM ('Core', 'Major', 'Minor', 'Emerging');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE tw.exposure_level AS ENUM ('High', 'Mid/High', 'Mid', 'Low');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE tw.exposure_type AS ENUM ('Direct', 'Indirect', 'Theme');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE tw.confidence_level AS ENUM ('A', 'B', 'C', 'D');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------- reference dictionaries ----------
-- Defined first: the exposure table below has a foreign key into one of them.
CREATE TABLE IF NOT EXISTS tw.supply_chain_position (
    position_name TEXT PRIMARY KEY,
    seq           TEXT,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS tw.supply_chain_layer (
    layer    TEXT PRIMARY KEY,          -- 上游 / 中游 / 下游
    coverage TEXT
);

CREATE TABLE IF NOT EXISTS tw.cycle_attributes (
    attribute                 TEXT PRIMARY KEY,   -- Growth / Cyclical / Defensive / Capex
    representative_industries TEXT
);

CREATE TABLE IF NOT EXISTS tw.region_exposure_rules (
    id      SERIAL PRIMARY KEY,
    section TEXT,
    item    TEXT,
    value   TEXT
);

-- R001-R021. Kept in the database rather than only in a spreadsheet so a
-- review can cite the rule it is applying.
CREATE TABLE IF NOT EXISTS tw.classification_rules (
    rule_id     VARCHAR(6) PRIMARY KEY,
    rule        TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS tw.manufacturing_mode_examples (
    id                 SERIAL PRIMARY KEY,
    company            TEXT,
    manufacturing_mode TEXT,
    product            TEXT
);

-- ---------- the tree ----------
-- Seeded at 356 nodes (26 L1 + 177 L2 + 153 L3) and maintained from
-- db/industry_tree_data.py since. Most branches stop at L2 on purpose;
-- only the branches we actually trade below the sub-industry level are cut
-- further. Adding a level-3 child to an L2 that has members is a migration,
-- not an edit: the members have to move down with it.
CREATE TABLE IF NOT EXISTS tw.industry_tree (
    node_code     VARCHAR(10) PRIMARY KEY,      -- 'A' | 'A1' | 'A1-01'
    parent_code   VARCHAR(10) REFERENCES tw.industry_tree(node_code),
    level         SMALLINT NOT NULL CHECK (level BETWEEN 1 AND 3),
    name          TEXT NOT NULL,
    main_products TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    CHECK ((level = 1) = (parent_code IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS industry_tree_path_key
    ON tw.industry_tree (parent_code, name) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_tw_industry_tree_parent
    ON tw.industry_tree (parent_code);

-- ---------- company-level attributes ----------
-- tw.stocks stays the identity table (listing state, name, official industry);
-- these are the judgement columns that would otherwise be overwritten nightly.
CREATE TABLE IF NOT EXISTS tw.company_profile (
    stock_id             VARCHAR(10) PRIMARY KEY REFERENCES tw.stocks(stock_id),
    cycle_attributes     TEXT,        -- comma separated, see tw.cycle_attributes
    main_market          TEXT,
    china_revenue        tw.exposure_level,
    us_revenue           tw.exposure_level,
    main_production_site TEXT,
    product_ownership    TEXT,        -- Own Chip / Design Service / IP Licensing / N/A
    cross_industry_note  TEXT,
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- ---------- industry exposure ----------
CREATE TABLE IF NOT EXISTS tw.company_industry_exposure (
    id                    SERIAL PRIMARY KEY,
    stock_id              VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    node_code             VARCHAR(10) NOT NULL REFERENCES tw.industry_tree(node_code),
    role                  tw.exposure_role NOT NULL,

    -- weight_item is the annual-report line this share was read off, named
    -- explicitly. The old fill script guessed the line from keywords and
    -- quietly matched side businesses (中美晶 filled 1.1% where the real line
    -- was 81.1%), so the item is now stated rather than inferred.
    weight_pct            NUMERIC(6,3),
    weight_item           TEXT,
    weight_source         TEXT,       -- 'annual_report_item' | 'estimate' | ...

    supply_chain_position TEXT REFERENCES tw.supply_chain_position(position_name),
    revenue_exposure      tw.exposure_level,
    profit_exposure       tw.exposure_level,
    strategic_exposure    tw.exposure_level,
    exposure_type         tw.exposure_type,
    confidence            tw.confidence_level,

    evidence              TEXT NOT NULL CHECK (btrim(evidence) <> ''),
    evidence_kind         TEXT NOT NULL CHECK (evidence_kind IN (
                              'annual_report_table', 'annual_report_prose',
                              'company_site', 'investor_conf',
                              'filing_too_coarse', 'no_filing', 'pending')),

    effective_from        DATE NOT NULL,
    effective_to          DATE,       -- NULL = still current
    known_date            DATE,       -- when this became knowable; backtests filter here
    created_at            TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (stock_id, node_code, effective_from),
    CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tw_cie_stock ON tw.company_industry_exposure (stock_id);
CREATE INDEX IF NOT EXISTS idx_tw_cie_node  ON tw.company_industry_exposure (node_code);
CREATE INDEX IF NOT EXISTS idx_tw_cie_role  ON tw.company_industry_exposure (stock_id, role);
CREATE INDEX IF NOT EXISTS idx_tw_cie_known ON tw.company_industry_exposure (known_date);

-- One Core per company in the current layer. A partial unique index rather
-- than a CHECK, because the constraint is about the layer, not about a row.
CREATE UNIQUE INDEX IF NOT EXISTS cie_one_current_core
    ON tw.company_industry_exposure (stock_id)
    WHERE role = 'Core' AND effective_to IS NULL;

-- ---------- themes (the narrative dimension) ----------
-- Independent of the tree on purpose, exactly as tw.theme_tags was: AI ASIC,
-- HBM and CoWoS have no line item in any filing, so they cannot be held to the
-- revenue rule the tree is held to. sub_theme carries the finer names the
-- T-code dictionary has no code for (CoWoS sits under T08 Advanced Packaging).
CREATE TABLE IF NOT EXISTS tw.theme_master (
    theme_code         VARCHAR(6) PRIMARY KEY,   -- T01..T55
    theme_group        TEXT,
    theme_name         TEXT NOT NULL,
    sub_theme          TEXT,
    related_industries TEXT,                     -- free text, e.g. 'A、C'
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tw.company_theme_mapping (
    id             SERIAL PRIMARY KEY,
    stock_id       VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    theme_code     VARCHAR(6)  NOT NULL REFERENCES tw.theme_master(theme_code),
    -- '' rather than NULL when the mapping is to the theme itself, so the
    -- uniqueness key below does not need NULLS NOT DISTINCT to behave.
    sub_theme      TEXT NOT NULL DEFAULT '',
    exposure_score SMALLINT CHECK (exposure_score BETWEEN 1 AND 5),

    evidence       TEXT NOT NULL CHECK (btrim(evidence) <> ''),
    evidence_kind  TEXT NOT NULL CHECK (evidence_kind IN (
                       'annual_report_table', 'annual_report_prose',
                       'company_site', 'investor_conf',
                       'filing_too_coarse', 'no_filing', 'pending')),
    confidence     tw.confidence_level,
    source         TEXT,

    effective_from DATE NOT NULL,
    effective_to   DATE,
    known_date     DATE,
    created_at     TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (stock_id, theme_code, sub_theme, effective_from),
    CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tw_ctm_stock ON tw.company_theme_mapping (stock_id);
CREATE INDEX IF NOT EXISTS idx_tw_ctm_theme ON tw.company_theme_mapping (theme_code);

-- ---------- groups and ecosystems (R014: these are not the same thing) ----------
-- A group is ownership: who controls whom. We already hold that as hard data in
-- tw.company_affiliations (MOPS 轉投資), so membership here is derived from it
-- rather than typed by hand; only the group's name and anchor are a judgement.
CREATE TABLE IF NOT EXISTS tw.business_group (
    group_code      VARCHAR(8) PRIMARY KEY,      -- 'G01'...
    group_name      TEXT NOT NULL,
    group_type      TEXT,
    anchor_stock_id VARCHAR(10) REFERENCES tw.stocks(stock_id),
    note            TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tw.company_group (
    id               SERIAL PRIMARY KEY,
    stock_id         VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    group_code       VARCHAR(8)  NOT NULL REFERENCES tw.business_group(group_code),
    relation_type    TEXT,
    linkage_strength SMALLINT CHECK (linkage_strength BETWEEN 1 AND 5),
    -- how the membership was derived: the affiliation path, or 'manual'
    derived_from     TEXT,
    effective_from   DATE NOT NULL,
    effective_to     DATE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, group_code, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tw_cg_stock ON tw.company_group (stock_id);
CREATE INDEX IF NOT EXISTS idx_tw_cg_group ON tw.company_group (group_code);

-- An ecosystem is not ownership: it is who a company sells to, builds on, or
-- allies with (NVIDIA, Apple, Tesla, MIH). Nothing in our data implies these,
-- so rows here are asserted and need a source like any other judgement.
CREATE TABLE IF NOT EXISTS tw.ecosystem_master (
    ecosystem_code        VARCHAR(8) PRIMARY KEY,   -- 'E01'...
    ecosystem_name        TEXT NOT NULL,
    ecosystem_type        TEXT,                     -- Customer / Platform / Alliance
    supply_chain_coverage TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tw.company_ecosystem_mapping (
    id                SERIAL PRIMARY KEY,
    stock_id          VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    ecosystem_code    VARCHAR(8)  NOT NULL REFERENCES tw.ecosystem_master(ecosystem_code),
    relationship_type TEXT,        -- Customer / Supplier / Platform Member / Alliance Member
    strength          SMALLINT CHECK (strength BETWEEN 1 AND 5),
    evidence          TEXT NOT NULL CHECK (btrim(evidence) <> ''),
    evidence_kind     TEXT,
    confidence        tw.confidence_level,
    source            TEXT,
    effective_from    DATE NOT NULL,
    effective_to      DATE,
    known_date        DATE,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, ecosystem_code, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tw_cem_stock ON tw.company_ecosystem_mapping (stock_id);
CREATE INDEX IF NOT EXISTS idx_tw_cem_eco   ON tw.company_ecosystem_mapping (ecosystem_code);

-- ---------- official classification ----------
-- tw.stocks.industry is today's official label, overwritten in place. This
-- table keeps the history of it, and the mapping table below is the bridge from
-- the exchange's 33 categories to our tree.
CREATE TABLE IF NOT EXISTS tw.company_official_classification (
    id                     SERIAL PRIMARY KEY,
    stock_id               VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    twse_official_industry TEXT,
    classification_source  TEXT,
    effective_from         DATE NOT NULL,
    effective_to           DATE,
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stock_id, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tw_coc_stock ON tw.company_official_classification (stock_id);

CREATE TABLE IF NOT EXISTS tw.twse_industry_mapping (
    twse_official_category TEXT PRIMARY KEY,
    master_l1              TEXT,
    master_l2_range        TEXT,      -- 'A1～A7' — a range, not a single node
    mapping_type           TEXT       -- 一對一 / 一對多 / ...
);

-- ---------- derived views (R019: never store what can be derived) ----------
CREATE OR REPLACE VIEW tw.v_company_core_business AS
SELECT DISTINCT ON (e.stock_id)
       e.stock_id, e.node_code, t.name AS core_business, e.effective_from
FROM tw.company_industry_exposure e
JOIN tw.industry_tree t ON t.node_code = e.node_code
WHERE e.role = 'Core' AND e.effective_to IS NULL
ORDER BY e.stock_id, e.effective_from DESC;

-- The sub-industry (L2) a company's Core sits under, which is the level most
-- consumers actually want — 'A3 晶圓製造' rather than 'Foundry / 先進製程'.
CREATE OR REPLACE VIEW tw.v_company_master_industry AS
SELECT DISTINCT ON (e.stock_id)
       e.stock_id,
       COALESCE(p.node_code, t.node_code) AS l2_code,
       COALESCE(p.name, t.name)           AS master_industry,
       t.name                             AS core_business,
       e.effective_from
FROM tw.company_industry_exposure e
JOIN tw.industry_tree t ON t.node_code = e.node_code
LEFT JOIN tw.industry_tree p ON p.node_code = t.parent_code AND t.level = 3
WHERE e.role = 'Core' AND e.effective_to IS NULL
ORDER BY e.stock_id, e.effective_from DESC;

CREATE OR REPLACE VIEW tw.v_company_master_industries AS
SELECT e.stock_id,
       array_agg(DISTINCT t.name ORDER BY t.name) AS master_industries
FROM tw.company_industry_exposure e
JOIN tw.industry_tree t ON t.node_code = e.node_code
WHERE e.effective_to IS NULL
GROUP BY e.stock_id;

CREATE OR REPLACE VIEW tw.v_company_main_theme AS
SELECT DISTINCT ON (stock_id)
       stock_id, theme_code, sub_theme, exposure_score
FROM tw.company_theme_mapping
WHERE effective_to IS NULL
ORDER BY stock_id, exposure_score DESC NULLS LAST;

CREATE OR REPLACE VIEW tw.v_company_official_classification_full AS
SELECT s.stock_id, s.name AS company_name,
       oc.twse_official_industry,
       mi.master_industries,
       mt.theme_code AS main_theme_code,
       tm.theme_name AS main_theme_name,
       oc.classification_source, oc.effective_from
FROM tw.stocks s
LEFT JOIN tw.company_official_classification oc
       ON oc.stock_id = s.stock_id AND oc.effective_to IS NULL
LEFT JOIN tw.v_company_master_industries mi ON mi.stock_id = s.stock_id
LEFT JOIN tw.v_company_main_theme mt ON mt.stock_id = s.stock_id
LEFT JOIN tw.theme_master tm ON tm.theme_code = mt.theme_code;
