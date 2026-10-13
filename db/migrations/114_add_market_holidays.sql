-- 114_add_market_holidays.sql
--
-- Exchange holiday schedule, so the trading-day gate has something to ask
-- other than "is there a TAIEX row for today".
--
-- daily_update.py treated a missing TAIEX row as a holiday. A fetch that fails
-- without raising also leaves no row, so an outage on a trading day looked
-- exactly like a holiday: the pipeline was skipped, the run exited 0, and the
-- health check, which took MAX(TAIEX) as the latest trading day, agreed that
-- nothing was missing (2026-09-18).
--
-- Source: TWSE OpenAPI holidaySchedule. It serves the current year only, so
-- rows are kept across years and never deleted. The list mixes closures with
-- markers for trading days ("first trading day after the new year"); is_closed
-- tells them apart. Unscheduled closures (typhoon days) are not in the source
-- and therefore not in this table.

CREATE TABLE IF NOT EXISTS tw.market_holidays (
    holiday_date DATE PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT,
    is_closed    BOOLEAN NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE tw.market_holidays IS
    'Exchange holiday schedule from TWSE OpenAPI holidaySchedule. Rows accumulate across years and are never deleted.';
COMMENT ON COLUMN tw.market_holidays.is_closed IS
    'FALSE for the entries that mark a trading day (first/last trading day around a holiday); TRUE for every other entry.';
