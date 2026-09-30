-- evidence_kind stops doubling as a reason, and the effective range gets the
-- comparison its own half-open definition implies.
--
-- 1. 'filing_too_coarse', 'no_filing' and 'pending' answer "why is there no
--    share", which is weight_status's question (migration 102). Keeping them in
--    evidence_kind meant one column held two namespaces: 欣興 read as
--    evidence_kind='filing_too_coarse' when the evidence was in fact the annual
--    report's revenue table. The 132 rows carrying the value were each restored
--    to the kind their own evidence text names (table vs prose) before this ran,
--    and every one of them already carried weight_status='Too_Coarse', so no
--    reason is lost. This CHECK fails loudly if any row still carries one --
--    that is the point: the data has to be fixed first, not silently coerced.
--
-- 2. cie_coarse_filing_has_no_weight (migration 100) said a coarse row may not
--    also claim a share. cie_weight_status_only_when_absent already says that
--    for every status, so the narrower one is now redundant.
--
-- 3. The validity period is [effective_from, effective_to): from = to is a
--    zero-length interval, not a one-day one. Two constraints said <= / >=,
--    which let it through and disagreed with R029's overlap test (touching
--    endpoints do not overlap). One constraint replaces both.

ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS company_industry_exposure_evidence_kind_check;
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT company_industry_exposure_evidence_kind_check
    CHECK (evidence_kind IN ('annual_report_table', 'annual_report_prose',
                             'company_site', 'investor_conf'));

ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS cie_coarse_filing_has_no_weight;

ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS cie_effective_range;
ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS company_industry_exposure_check;
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_effective_range
    CHECK (effective_to IS NULL OR effective_from < effective_to);
