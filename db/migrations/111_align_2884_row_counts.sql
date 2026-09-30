-- Migration 110 retracted 2884 玉山金's four rows and set the status back to
-- no_section, but left persisted_rows at 4. The row-integrity check counts live
-- rows, so it has reported the company ever since: persisted_rows 4 against
-- actual_rows 0. The disagreement is real and it is this migration's own doing
-- -- a retraction changes what the status row describes, so the counters it
-- carries have to move with it.
--
-- parsed_rows stays at 4 on purpose. It records what the parser produced from
-- that document, which is still true and is the reason the retraction exists;
-- persisted_rows records how many of those are evidence, which is now none.

UPDATE tw.product_revenue_status
   SET persisted_rows = 0
 WHERE fiscal_year = 2025 AND stock_id = '2884';
