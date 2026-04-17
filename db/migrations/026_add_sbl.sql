-- Securities Borrowing and Lending (SBL / 借券賣出) per-stock daily balance.
-- Source: TWSE TWT93U + TPEx sbl endpoint.
-- All values in shares (股), not lots (張).
-- 借券賣出 differs from 融券 (margin short) — used by foreign/institutional
-- to short large positions beyond margin limits.

ALTER TABLE tw.daily_prices
    ADD COLUMN IF NOT EXISTS sbl_prev_balance BIGINT,
    ADD COLUMN IF NOT EXISTS sbl_sell         BIGINT,
    ADD COLUMN IF NOT EXISTS sbl_return       BIGINT,
    ADD COLUMN IF NOT EXISTS sbl_adjust       BIGINT,
    ADD COLUMN IF NOT EXISTS sbl_balance      BIGINT,
    ADD COLUMN IF NOT EXISTS sbl_limit        BIGINT;
