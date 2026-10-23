-- tw.market_holidays gets the row's source. 'twse_calendar' is the exchange's
-- published schedule (scrapers.market_holidays, every row before this
-- migration but one); 'intraday_detected' is written by intraday.sweeper when
-- a weekday session shows no trade stamped that day (typhoon closures are not
-- in the published schedule); 'manual' is entered by hand. 2026-10-23, the
-- typhoon closure that prompted the detection, was entered by hand.
ALTER TABLE tw.market_holidays
    ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'twse_calendar';

ALTER TABLE tw.market_holidays DROP CONSTRAINT IF EXISTS market_holidays_source_check;
ALTER TABLE tw.market_holidays ADD CONSTRAINT market_holidays_source_check
    CHECK (source IN ('twse_calendar', 'intraday_detected', 'manual'));

UPDATE tw.market_holidays SET source = 'manual'
 WHERE holiday_date = DATE '2026-10-23';

COMMENT ON TABLE tw.market_holidays IS
    'Exchange closures: the TWSE OpenAPI holidaySchedule plus unscheduled closures (typhoon days) detected intraday or entered by hand. Rows accumulate across years and are never deleted, except an intraday_detected row the sweeper withdraws when trades appear.';
COMMENT ON COLUMN tw.market_holidays.source IS
    'twse_calendar (scrapers.market_holidays), intraday_detected (intraday.sweeper: no trade stamped today by 09:05), manual.';
