-- A filing too coarse to split cannot also carry a share.
--
-- R020② says a revenue line spanning several nodes leaves every one of them at
-- weight_pct NULL with evidence_kind 'filing_too_coarse'. That has been true of
-- all 122 such rows, but only because db.fill_weight happens to skip them --
-- nothing stopped a hand-written weight from landing on one, and a weight there
-- is precisely the failure the rule exists to prevent: 69.86% split into 40 and
-- 29.86 because the numbers look plausible, coverage rising while the data gets
-- worse.
--
-- The 2026-09-04 A2 adjudication is why this is worth making structural. Two
-- different things produce a NULL weight and only one of them should ever go
-- away: a taxonomy collision, where one product satisfies two siblings because
-- their definitions use different axes (fix the tree, R031), and a filing
-- limitation, where the tree is clear but the company genuinely works both
-- sides and reports one combined line (keep the NULL, permanently). 3131 弘塑,
-- 3581 博磊 and 3583 辛耘 were the first kind and are now the second; the rows
-- did not disappear, their reason changed.
--
-- So this constrains the second kind rather than counting it: whatever else
-- moves, a row that says the filing could not be split may not also claim to
-- know the share.

ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_coarse_filing_has_no_weight
    CHECK (evidence_kind <> 'filing_too_coarse' OR weight_pct IS NULL);
