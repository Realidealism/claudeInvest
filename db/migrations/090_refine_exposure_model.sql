-- Four corrections to the exposure model, from a review of the rule set.
--
-- 1. Theme leaves tw.exposure_type. R003 already says a theme cannot be a
--    primary industry, and themes have their own table with their own contract
--    (no revenue test, evidence required). Leaving 'Theme' as a third exposure
--    type invited a row that says "this industry exposure is actually a theme",
--    which contradicts the separation the two tables exist to enforce. No row
--    ever used it.
--
-- 2. weight_pct gains a type and a basis. A bare "30%" is not comparable across
--    rows: it may be a share of consolidated revenue, of capacity, or a
--    qualitative read of strategic weight. Every share we hold today is a
--    revenue share off the annual report's 營業比重 table, which is consolidated
--    — recorded rather than assumed from here on.
--
-- 3. evidence_period_end records which period the evidence covers, which is a
--    third thing from when the fact holds (effective_from/to) and when we could
--    have known it (known_date). An FY2025 annual report evidences the year
--    ending 2025-12-31, was filed in mid-2026, and describes a classification
--    that still holds now.
--
--    effective_to deliberately stays NULL for current rows. Dating the validity
--    window to the fiscal year instead would expire every row at the year end
--    and empty the current layer.
--
-- 4. Rules get a category, so 22 of them read as a structure rather than a list.
ALTER TYPE tw.exposure_type RENAME TO exposure_type_old;
CREATE TYPE tw.exposure_type AS ENUM ('Direct', 'Indirect');
ALTER TABLE tw.company_industry_exposure
    ALTER COLUMN exposure_type TYPE tw.exposure_type
    USING exposure_type::text::tw.exposure_type;
DROP TYPE tw.exposure_type_old;

DO $$ BEGIN
    CREATE TYPE tw.weight_type AS ENUM ('Revenue', 'Profit', 'Capacity', 'Strategic');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE tw.company_industry_exposure
    ADD COLUMN IF NOT EXISTS weight_type tw.weight_type,
    ADD COLUMN IF NOT EXISTS weight_basis TEXT,
    ADD COLUMN IF NOT EXISTS evidence_period_end DATE;

COMMENT ON COLUMN tw.company_industry_exposure.weight_type IS
    'What weight_pct measures; NULL only when weight_pct is NULL';
COMMENT ON COLUMN tw.company_industry_exposure.weight_basis IS
    'The denominator, e.g. consolidated_revenue';
COMMENT ON COLUMN tw.company_industry_exposure.evidence_period_end IS
    'Last day of the period the evidence covers; distinct from known_date';

-- Backfill before the constraint below can hold. Every share currently in the
-- table was read off the annual report's 營業比重 table, whose denominator is
-- consolidated revenue; evidence dates to FY2025 wherever the annual report is
-- the source. Rows evidenced by a company site or an investor conference get no
-- period — those sources do not carry one, and inventing 2025-12-31 for them
-- would be a guess dressed as a fact.
UPDATE tw.company_industry_exposure
   SET weight_type = 'Revenue', weight_basis = 'consolidated_revenue'
 WHERE weight_pct IS NOT NULL AND weight_type IS NULL;

UPDATE tw.company_industry_exposure
   SET evidence_period_end = DATE '2025-12-31'
 WHERE evidence_period_end IS NULL
   AND evidence_kind IN ('annual_report_table', 'annual_report_prose', 'filing_too_coarse');

-- A share with no stated basis is not comparable, so the two travel together.
ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS cie_weight_typed;
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_weight_typed
    CHECK (weight_pct IS NULL OR weight_type IS NOT NULL);

ALTER TABLE tw.classification_rules
    ADD COLUMN IF NOT EXISTS category TEXT;
