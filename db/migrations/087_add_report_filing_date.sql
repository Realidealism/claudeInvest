-- When the annual report behind a classification actually reached the public.
--
-- Every membership in tw.company_industry_exposure is evidenced by an FY2025
-- annual report, and R021 wants known_date to be the day that evidence became
-- available — otherwise a backtest that reads the classification is using
-- knowledge from the future. We had no such date: the closest thing was our own
-- updated_at, which says when the scraper ran.
--
-- MOPS already returns it. The t57sb01 filing list that annual_report_revenue
-- .find_report() posts to prints the upload timestamp next to each filename
-- (2330's FY2025 report reads 115/05/21 19:02:58), so this needs no new
-- endpoint — only that the response stops being thrown away.
ALTER TABLE tw.product_revenue_status
    ADD COLUMN IF NOT EXISTS filing_date DATE;

COMMENT ON COLUMN tw.product_revenue_status.filing_date IS
    'MOPS upload timestamp of source_file, date part only; NULL = not yet backfilled';
