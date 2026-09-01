-- Separate what we measured from what we judged from what follows from both.
--
-- The V3.7 draft gave every exposure three qualitative levels — revenue, profit
-- and strategic — alongside a confidence grade. All four sat empty in all 449
-- rows, and a column that is always NULL invites the next maintainer to fill it
-- by guessing. Reviewing them one at a time, they turned out to be three
-- different problems wearing the same clothes.
--
--   revenue_exposure  is derivable. A share of 44.98% is Medium under any fixed
--                     banding, so storing the band is storing a second copy of
--                     weight_pct that can drift from it. It becomes a view.
--
--   profit_exposure   has no source. Taiwanese annual reports disclose segment
--                     revenue, not segment profit, and nothing in this database
--                     carries it. R020 forbids guessing, so the column goes
--                     rather than waiting to be guessed at. Reintroduce it when
--                     segment profit actually exists.
--
--   strategic_exposure is the one that adds information — 欣興's ABF expansion
--                     is not visible in any revenue share — but the name reads
--                     as "how much revenue is at stake", which is the opposite
--                     of what it records. It becomes strategic_priority, and
--                     like every other judgement here it must carry its reason.
--
--   confidence        stays, and is explicitly NOT a restatement of
--                     evidence_kind (R018). They answer different questions:
--                     evidence_kind says what kind of source this came from,
--                     confidence says how sure we are of the claim given that
--                     source. 欣興's「印刷電路板 99.07%」is excellent evidence
--                     and still supports no confident allocation to one node —
--                     high-quality source, unavailable allocation. Mapping one
--                     column onto the other would erase exactly that case.
--
-- Nothing populates strategic_priority yet. When something does, db.seed_industry
-- must learn to carry it: the loader supersedes a row by closing and reinserting,
-- and would drop any column it does not know about.
ALTER TABLE tw.company_industry_exposure
    DROP COLUMN IF EXISTS revenue_exposure,
    DROP COLUMN IF EXISTS profit_exposure;

DO $$ BEGIN
    -- 'Unknown' is not a synonym for NULL here: NULL means nobody has looked,
    -- 'Unknown' means someone looked and the filing does not say. Same
    -- distinction evidence_kind draws with 'filing_too_coarse'.
    CREATE TYPE tw.priority_level AS ENUM ('High', 'Medium', 'Low', 'Unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE tw.company_industry_exposure
    RENAME COLUMN strategic_exposure TO strategic_priority;
ALTER TABLE tw.company_industry_exposure
    ALTER COLUMN strategic_priority TYPE tw.priority_level
    USING strategic_priority::text::tw.priority_level;
ALTER TABLE tw.company_industry_exposure
    ADD COLUMN IF NOT EXISTS strategic_evidence TEXT;

COMMENT ON COLUMN tw.company_industry_exposure.strategic_priority IS
    'How much the company itself is steering toward this node, not how much '
    'revenue it earns there; sourced from the annual report narrative';
COMMENT ON COLUMN tw.company_industry_exposure.confidence IS
    'Confidence in THIS exposure claim, not the quality of the source. '
    'Not derivable from evidence_kind (R018)';

-- A stated priority is a judgement, so it carries its reason, exactly as a
-- stated share carries its type.
ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS cie_priority_reasoned;
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_priority_reasoned
    CHECK (strategic_priority IS NULL
           OR strategic_priority = 'Unknown'
           OR btrim(COALESCE(strategic_evidence, '')) <> '');

-- The analysis layer. Bands are fixed here so every consumer reads the same
-- ones, and a share that is not a revenue share yields no band at all rather
-- than a misleading one.
CREATE OR REPLACE VIEW tw.v_company_industry_exposure AS
SELECT e.id, e.stock_id, s.name AS company_name, s.industry AS official_industry,
       e.node_code, t.name AS node_name, t.level AS node_level,
       p.node_code AS parent_code, p.name AS parent_name,
       e.role, e.exposure_type,
       e.weight_pct, e.weight_type, e.weight_basis, e.weight_item,
       CASE
           WHEN e.weight_type IS DISTINCT FROM 'Revenue' THEN NULL
           WHEN e.weight_pct IS NULL THEN NULL
           WHEN e.weight_pct >= 50 THEN 'High'
           WHEN e.weight_pct >= 20 THEN 'Medium'
           ELSE 'Low'
       END AS revenue_exposure,
       e.strategic_priority, e.strategic_evidence,
       e.evidence, e.evidence_kind, e.confidence,
       e.effective_from, e.effective_to, e.known_date, e.evidence_period_end
FROM tw.company_industry_exposure e
JOIN tw.stocks s ON s.stock_id = e.stock_id
JOIN tw.industry_tree t ON t.node_code = e.node_code
LEFT JOIN tw.industry_tree p ON p.node_code = t.parent_code;
