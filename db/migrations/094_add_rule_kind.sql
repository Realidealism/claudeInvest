-- Separate what a rule is about from what a rule does.
--
-- category already says the subject (產業分類, 題材, 權重與證據 …). It says
-- nothing about how a reader is supposed to act on the rule, and the three ways
-- are very different: a hard constraint decides whether a row may exist at all,
-- a decision rule needs a human to weigh evidence, and a validation rule only
-- reports. Code that wants to enforce the first kind and merely surface the
-- third had no way to tell them apart.
ALTER TABLE tw.classification_rules
    ADD COLUMN IF NOT EXISTS rule_kind TEXT
    CHECK (rule_kind IN ('hard_constraint', 'decision', 'validation'));

COMMENT ON COLUMN tw.classification_rules.rule_kind IS
    'hard_constraint: violating it means the row must not exist (DB constraint '
    'or loader refusal). decision: needs judgement against evidence; no check '
    'can settle it. validation: reports a finding and lets the load proceed.';
