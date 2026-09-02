-- Move two rules out of the prose and into the database.
--
-- R006 says the Profit weight type is unusable — Taiwanese annual reports do
-- not disclose segment profit — but the enum still carried the label and the
-- loader could write it. A policy the implementation does not share is a policy
-- that lasts until someone reaches for the obvious column.
--
-- R030 says the three time axes have to be consistent with each other. All four
-- conditions hold across the 541 rows today, so the constraints are recording an
-- invariant rather than asking for a migration of existing data.
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_no_profit_weight
    CHECK (weight_type IS NULL OR weight_type <> 'Profit');

ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_effective_range
    CHECK (effective_to IS NULL OR effective_from <= effective_to);

-- known_date is when the evidence could first have been read. It cannot fall
-- after the row stopped being current, or a backtest would first see the row in
-- a window where it no longer applies.
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_known_within_range
    CHECK (effective_to IS NULL OR known_date IS NULL OR known_date <= effective_to);

-- The period the evidence covers ends before the evidence is published.
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_period_before_known
    CHECK (evidence_period_end IS NULL OR known_date IS NULL
           OR evidence_period_end <= known_date);
