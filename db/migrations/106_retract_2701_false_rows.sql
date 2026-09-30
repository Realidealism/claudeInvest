-- Retract two rows whose provenance was disproved, not two rows we failed to read.
--
-- 2701 萬企's FY2025 mix held 「357仟元 87.0」 and 「646仟元 13.0」. Those are not
-- product names: the prose reader's item class allows digits (it must, for names
-- like 5G計算平台), so a capture could begin inside a monetary amount and end on
-- its unit, and _is_label passed it because 仟元 is two non-digit characters.
-- The rows were produced by the 2026-09-09 page-filter change, identified in the
-- 237-company regression diff, and the reader was fixed the same day by naming
-- 仟元/千元/百萬元/億元 in PROSE_NOISE. The company now parses to nothing and sits
-- at no_section.
--
-- The distinction this migration turns on, and the reason it is not a general
-- rule:
--
--   retracted because no_section  -- we could not read the filing this time, so
--                                    the last known rows are KEPT. They may
--                                    still be right, and a fetch or locator
--                                    failure must not empty a company.
--   retracted because falsified   -- a later parse and a named defect show how
--                                    these particular rows were manufactured.
--                                    "Last known" has no standing once we know
--                                    where the value came from.
--
-- Only the second applies here, and only to this company. A wider "delete on
-- parse failure" rule would be the first kind, which stays forbidden: a visible
-- gap beats a silent wrong basis, but a KNOWN-wrong basis is not a gap -- it is
-- data a downstream reader would use.
DELETE FROM tw.product_revenue
 WHERE stock_id = '2701' AND fiscal_year = 2025
   AND item IN ('357仟元', '646仟元');

UPDATE tw.product_revenue_status
   SET detail = 'rows retracted 2026-09-10 as falsified: 「357仟元 87.0」/「646仟元 13.0」 '
                'were amount suffixes captured as product names by the 2026-09-09 '
                'page-filter change; reader fixed by naming currency units in '
                'PROSE_NOISE. Not a no_section retention case -- the provenance was '
                'disproved, not merely unreadable.'
 WHERE stock_id = '2701' AND fiscal_year = 2025;
