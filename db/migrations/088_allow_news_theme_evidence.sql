-- Let a theme membership cite news; an industry-chain membership still cannot.
--
-- The two tables are held to different standards on purpose. tw.company_indust
-- ry_exposure claims a company earns revenue from a part of a supply chain, and
-- that claim is checkable against a filing, so its evidence must come from one.
-- A theme claims the market prices the company on a story — AI ASIC, HBM, CoWoS
-- have no line item anywhere — and for the speculative tier the honest source
-- often IS reporting: 12 of the inherited rows say so plainly (光罩: 先進節點光罩
-- 以客戶自製為主).
--
-- Refusing the value would not raise the standard, only push those rows to
-- claim a stronger source than they have.
ALTER TABLE tw.company_theme_mapping
    DROP CONSTRAINT IF EXISTS company_theme_mapping_evidence_kind_check;

ALTER TABLE tw.company_theme_mapping
    ADD CONSTRAINT company_theme_mapping_evidence_kind_check
    CHECK (evidence_kind IN (
        'annual_report_table', 'annual_report_prose',
        'company_site', 'investor_conf', 'news',
        'filing_too_coarse', 'no_filing', 'pending'));
