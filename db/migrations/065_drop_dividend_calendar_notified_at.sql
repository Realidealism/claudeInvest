-- Drop tw.dividend_calendar.notified_at.
--
-- It existed to guarantee each (stock_id, ex_date) was announced exactly once.
-- The morning brief now repeats the ex-dividend alert every day from D-3 until
-- the ex-date, so there is nothing to dedup and no reader of this column.

ALTER TABLE tw.dividend_calendar DROP COLUMN IF EXISTS notified_at;
