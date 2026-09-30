-- role ranks; it does not describe business status.
--
-- tw.exposure_role is ('Core','Major','Minor','Emerging'). The first three are
-- a derived ranking of how much a business matters to a company; the fourth is
-- a fact about the business itself -- that it has been announced or invested in
-- but has no identifiable revenue yet. R019 has said so in words since it was
-- written, and had to implement its own candidate rule as
-- `role IS DISTINCT FROM 'Emerging'` while noting that "乾淨的作法是另立
-- business_status 欄把狀態與排名分開" and that no row uses the value.
--
-- No row has ever used it. So the cheap correction is not to add a
-- business_status column -- across 714 rows it would be the constant
-- 'Operating' and describe nothing -- and not to drop the enum value either:
-- tw.v_company_industry_exposure exposes the column, so removing a value means
-- recreating the type and every dependent view for a change that alters no
-- data. A CHECK forbids the wrong write at the same strength, touches no view,
-- and is one line to lift.
--
-- When a real Emerging case appears, the answer is a business_status column and
-- the removal of this constraint -- not writing the status back into role. That
-- is the whole point of forbidding it now, while the cost of being wrong is nil.

ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_role_is_a_ranking
    CHECK (role <> 'Emerging');

COMMENT ON CONSTRAINT cie_role_is_a_ranking ON tw.company_industry_exposure IS
    'role holds a ranking (Core/Major/Minor). Business status belongs in its own '
    'column -- see R019 and migration 103. Lift this only together with adding it.';
