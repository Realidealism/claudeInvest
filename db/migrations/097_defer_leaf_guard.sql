-- Let the leaf guard judge the end state instead of the moment mid-way.
--
-- Migration 096 added industry_tree_leaf_guard as a BEFORE INSERT trigger: it
-- refuses a child while the parent still carries current exposures, so that a
-- new level cannot silently strand its members on a branch node (R016).
--
-- The invariant is right; the moment it is checked is not. Splitting a leaf that
-- has members is a legal, necessary operation — V1 半導體通路 holds 30 of them and
-- wants to become 積體電路通路 / 被動元件通路 / 設備代理 — and it is unreachable
-- under a BEFORE INSERT check. The exposures may only move to the children, the
-- children may not exist while the exposures are there, and so the split cannot
-- be expressed at all. What the trigger rejects is not a broken database but an
-- intermediate state on the way to a correct one.
--
-- Deferring it to commit keeps the guarantee — no transaction may END with
-- exposures on a branch node — while allowing the two halves of a split to
-- happen together. A caller that inserts the children and forgets to move the
-- members still fails, just at COMMIT rather than at the INSERT.
--
-- CONSTRAINT TRIGGER only exists in the AFTER form, which is what deferral
-- requires anyway; the function body is unchanged and its return value is
-- ignored in an AFTER trigger.
-- The message told the caller to move the exposures "first", which is now the
-- one order that does not work.
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
            '(R016 — move them onto leaf nodes in the same transaction)',
            NEW.node_code, NEW.parent_code, n;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS industry_tree_leaf_guard ON tw.industry_tree;

CREATE CONSTRAINT TRIGGER industry_tree_leaf_guard
    AFTER INSERT ON tw.industry_tree
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION tw.industry_tree_leaf_guard();
