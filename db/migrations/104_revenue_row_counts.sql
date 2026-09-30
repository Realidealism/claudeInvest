-- What a parse produced, what actually landed, and how many collided.
--
-- tw.product_revenue's primary key is (stock_id, fiscal_year, item), and save()
-- upserts row by row. Two parsed rows carrying the same item text therefore
-- collapse into one with no error, while the status row recorded the parsed
-- count -- so 2338 光罩 read "6 rows" beside a single stored row for two weeks
-- and nothing could see it. Free text could not be checked; these can.
--
-- persisted_rows is the one a checker compares against COUNT(*). parsed_rows
-- and duplicate_rows are kept because "parsed 6, persisted 1, 5 duplicates" is
-- an explainable state, while "persisted 1" alone hides that five were lost.
ALTER TABLE tw.product_revenue_status
    ADD COLUMN parsed_rows    integer,
    ADD COLUMN persisted_rows integer,
    ADD COLUMN duplicate_rows integer;

COMMENT ON COLUMN tw.product_revenue_status.parsed_rows IS
    'rows the parser emitted for this filing';
COMMENT ON COLUMN tw.product_revenue_status.persisted_rows IS
    'rows actually written to tw.product_revenue; a checker compares this to COUNT(*)';
COMMENT ON COLUMN tw.product_revenue_status.duplicate_rows IS
    'parsed_rows - persisted_rows: parsed rows whose item text repeated an earlier one';

-- Left NULL for existing rows on purpose. Backfilling them from today's COUNT(*)
-- would assert that the stored rows are what the parse produced, which is the
-- very thing that turned out to be false for 84 companies. They fill in when
-- each filing is re-parsed.
