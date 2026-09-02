-- Undo cie_known_within_range: it was wrong about what the two axes mean.
--
-- effective_to says when the classification stopped being true; known_date says
-- when we could first have read it. A business that ran through 2020 and was
-- disclosed in the FY2020 annual report filed in March 2021 has effective_to
-- 2020-12-31 and known_date 2021-03-15, and that is exactly the case known_date
-- exists to record: a real fact about 2020 that no backtest standing in 2020
-- may use. The constraint would have refused it.
--
-- Nothing in the table violates it today, because effective_to is only ever set
-- at supersede time and known_date is a past filing date. It would have bitten
-- the first time real historical layers were loaded.
ALTER TABLE tw.company_industry_exposure
    DROP CONSTRAINT IF EXISTS cie_known_within_range;

-- Adding a child demotes its parent from leaf, and R016 only allows exposures on
-- leaves. Doing it while exposures still point at the parent leaves them
-- semantically stranded — the FK still resolves, so nothing complains until
-- check_industry's [depth] finds them afterwards. Refuse the insert instead, so
-- the migration of those exposures has to happen first.
CREATE OR REPLACE FUNCTION tw.industry_tree_leaf_guard() RETURNS trigger AS $$
DECLARE
    n INTEGER;
BEGIN
    IF NEW.parent_code IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT count(*) INTO n
    FROM tw.company_industry_exposure
    WHERE node_code = NEW.parent_code AND effective_to IS NULL;
    IF n > 0 THEN
        RAISE EXCEPTION
            'cannot add child % under %: % current exposures still point at it '
            '(R016 — move them to leaf nodes first)',
            NEW.node_code, NEW.parent_code, n;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS industry_tree_leaf_guard ON tw.industry_tree;
CREATE TRIGGER industry_tree_leaf_guard
    BEFORE INSERT ON tw.industry_tree
    FOR EACH ROW EXECUTE FUNCTION tw.industry_tree_leaf_guard();
