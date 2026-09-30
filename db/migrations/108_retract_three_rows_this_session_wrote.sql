-- Retract the rows a parser change wrote and then disproved, in this session.
--
-- A persistence run for two accepted parser fixes was stopped after 26 of 265
-- companies because two of its five "rescues" were wrong:
--
--   1338 廣華-KY  its own mix table was correctly rejected (the read was
--                 missing three merged sub-rows and summed to 34.54), and the
--                 amounts-only fallback then walked on to page 94 and stored
--                 五 被投資公司名稱 -- 星禧科技, 廣佳投資, 廣泰投資, 智景控股,
--                 HIROTAI墨西哥 -- as the company's product mix, status ok.
--   1413 宏洲     「之製造、行銷所佔營業比重為 100%」 read as 「行銷所 100%」,
--   1452 宏益     because 所佔 was missing from the prose connector set.
--
-- These are not retained-on-failure rows: their provenance is disproved, the
-- same ground as migration 106. Each has since been fixed in the parser and
-- covered by tests (tests/test_revenue_fallback_scope.py), and each company is
-- re-parsed by the resumed run; deleting here is what makes that re-parse
-- meaningful, because save() only replaces rows when the new read succeeds --
-- a company that correctly reads nothing would otherwise keep these forever.
--
-- 1338 also loses the rows it held before this session's run: those were
-- deleted by the same run and were themselves wrong (30.29 + 4.25 + a 100%
-- total row, with 22.54, 37.34 and 5.58 never read). The honest state is no
-- rows, not a restored bad read.

DELETE FROM tw.product_revenue
 WHERE fiscal_year = 2025
   AND stock_id IN ('1338', '1413', '1452');

UPDATE tw.product_revenue_status
   SET status = 'no_section',
       detail = 'rows retracted by migration 108; awaiting re-parse'
 WHERE fiscal_year = 2025
   AND stock_id IN ('1338', '1413', '1452');
