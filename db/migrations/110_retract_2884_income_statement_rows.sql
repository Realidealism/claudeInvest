-- 2884 玉山金: an income statement read as a product mix.
--
-- The filing-selection fix moved this company from the 「僅永續專章」 edition
-- (1.6MB, February) to the full annual report (7.3MB, May). The source is now
-- right. What the correction exposed is that on a financial holding company's
-- layout the amounts-only fallback finds a different table: 手續費收入,
-- 利息收入, 金融商品損益, 其他營業淨收入(損失) -- the four lines of a bank's
-- income statement, with no percentages at all -- and the company was stored
-- ok with those as its revenue mix.
--
-- This is not a selection defect. It is the same family as 1338 廣華's
-- 被投資公司 table and 4726's P&L rows: the fallback is scoped to the first
-- table-bearing page under the section, and a bank's section is laid out
-- differently. Fixing that is parser work and belongs to its own batch, where
-- its blast radius can be measured; doing it here would mean this batch's
-- downstream differences could no longer be attributed to the source change
-- alone.
--
-- So the rows are retracted rather than kept or parsed away. They stay
-- auditable, they stop being evidence, and the company returns to the honest
-- state it was in before -- no readable mix -- but now with the right source
-- file recorded against it.

UPDATE tw.product_revenue
   SET retracted_at = NOW(),
       retracted_reason = 'income-statement captions read as a product mix; '
                          'retracted by migration 110 pending the amounts-only '
                          'fallback fix (same family as 1338, 4726)'
 WHERE fiscal_year = 2025
   AND stock_id = '2884'
   AND retracted_at IS NULL;

UPDATE tw.product_revenue_status
   SET status = 'no_section',
       detail = 'source corrected to the full annual report (F18, uploaded '
                '2026-05-12); its mix section is still unread -- the four rows '
                'the fallback produced were the bank income statement and are '
                'retracted (migration 110)'
 WHERE fiscal_year = 2025 AND stock_id = '2884';
