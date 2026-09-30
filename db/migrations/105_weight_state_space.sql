-- Two states the original three could not express, both found by adjudicating
-- the 94 unexplained NULLs on 2026-09-09.
--
-- Not_Extracted: the source exists and we did not get the number out of it.
--   'Not_Disclosed' asserts something about the company; using it here would
--   have been false for 21 of those 94. 3006 晶豪科's filing says
--   「積體電路(IC)銷售收入…占營業比率為 100%」 and 5347 世界's says
--   「晶圓收入 46,763,229」 -- ours broke on (cid:3) placeholders, and 4989 榮科's
--   digits extracted as 「����,���」. This is a WORK QUEUE, not a verdict: when
--   the extraction is fixed the row must be re-adjudicated into a weight,
--   Too_Coarse or Not_Disclosed, never left sitting here.
--
-- Disclosure_Invalid: the company disclosed a figure and the figure is wrong.
--   4721 恒耀's two lines are 59% and 51%, which sum to 110%; the amounts imply
--   49/51. Calling that "not disclosed" would hide that a number exists and is
--   unusable -- a different follow-up from either silence or coarseness.
ALTER TYPE tw.weight_status ADD VALUE IF NOT EXISTS 'Not_Extracted';
ALTER TYPE tw.weight_status ADD VALUE IF NOT EXISTS 'Disclosure_Invalid';
