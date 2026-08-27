-- Store the authoritative TWSE/TPEx index snapshot alongside the h(t) profile.
--
-- market_total_value is the sweeper's own sum of per-stock total_value, whose
-- unit does not match published turnover (its ratio to the official figure
-- drifts ~2% day to day), so it can only be used for ratios — never reported
-- as an absolute amount. The E.Sun market-data API exposes IX0001 (發行量加權
-- 股價指數) and IX0043 (櫃買指數), each carrying the exchange's own cumulative
-- trade value plus the index level and change percent. Recording those per
-- 5-minute bucket gives the intraday volume brief real NTD figures and a market
-- direction line, and starts accumulating a same-source history so the h(t)
-- curve can later be rebuilt on the authoritative series.
ALTER TABLE tw.intraday_value_profile
    ADD COLUMN IF NOT EXISTS twse_trade_value BIGINT,
    ADD COLUMN IF NOT EXISTS tpex_trade_value BIGINT,
    ADD COLUMN IF NOT EXISTS taiex_close NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS taiex_change_pct NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS tpex_close NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS tpex_change_pct NUMERIC(6,2);
