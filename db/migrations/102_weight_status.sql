-- Why a weight is absent is not the same question as what the evidence was.
--
-- R020 says it plainly: "NULL 本身不是錯誤狀態，沒有正確原因的 NULL 才是." The
-- schema could not carry that. 225 of 714 current rows have weight_pct NULL,
-- and only 131 of them say why -- those are the ones whose evidence_kind
-- happens to be 'filing_too_coarse', a value that doubles as a reason. The
-- other 94 carry annual_report_prose (73), company_site (10),
-- annual_report_table (9) or investor_conf (2): all of those answer "where did
-- this judgement come from", none answers "so why is there no number".
--
-- Those 94 are not a backlog this migration clears. It has no way to know their
-- reasons -- each needs its filing read. Leaving them NULL is the point: the
-- column makes them visible as rows whose NULL has no recorded cause, which is
-- exactly the state R020 calls the defect.
--
-- The column answers one question only -- why is there no number -- so it is
-- itself NULL whenever the question does not arise. There is deliberately no
-- 'Quantified' value: a row with a weight has no reason-for-absence to record,
-- and adding one would make every quantified row carry a token that says
-- nothing. Two axes that were proposed for this enum are also deliberately
-- absent: 'Indirect' is exposure_type's value and 'Emerging' is a business
-- status. Putting either here would repeat the defect that role already has,
-- one column holding two kinds of fact.

CREATE TYPE tw.weight_status AS ENUM (
    'Too_Coarse',      -- the filing combines several activities in one line
    'Not_Disclosed',   -- the activity is real, the filing gives no share at all
    'Not_Applicable'   -- a share of revenue is not the right measure here
);

ALTER TABLE tw.company_industry_exposure
    ADD COLUMN weight_status tw.weight_status;

-- A row that has a share has nothing to explain.
ALTER TABLE tw.company_industry_exposure
    ADD CONSTRAINT cie_weight_status_only_when_absent
    CHECK (weight_pct IS NULL OR weight_status IS NULL);

-- The only reason that can be derived mechanically: evidence_kind already
-- asserts it, and cie_coarse_filing_has_no_weight already guarantees these
-- rows carry no weight.
UPDATE tw.company_industry_exposure
   SET weight_status = 'Too_Coarse'
 WHERE evidence_kind = 'filing_too_coarse'
   AND weight_pct IS NULL;

COMMENT ON COLUMN tw.company_industry_exposure.weight_status IS
    'Why weight_pct is absent. NULL when weight_pct is present (nothing to explain) '
    'or when the reason has not been adjudicated yet -- the latter is reported by '
    'db.check_industry [weight_reason].';
