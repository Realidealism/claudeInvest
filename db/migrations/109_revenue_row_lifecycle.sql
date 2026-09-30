-- A revenue row outlives the read that produced it, and nothing said so.
--
-- save() replaces a company's rows only when the new parse succeeds. That rule
-- exists so a failed download cannot empty a company that already had a good
-- read, and it must stay. But it left a second case unhandled: a parse that
-- SUCCEEDS in opening the document and finds no mix section. There the old rows
-- are not merely old -- the same document, re-read, no longer supports them.
--
-- 23 companies were in that state. Their stored rows are what earlier parser
-- versions produced before the context gate existed, and they are still live
-- evidence: 8401 白紗科 holds 「整體印刷業銷售總值(仟元)」 and 「市場佔有率(%)」,
-- 9941 裕融 holds 「汽車分期設定台數」, 3577 泓格 holds 「研發費用」, 9911 櫻花
-- holds 「櫻花投入數位強排產品研發超過」, 8487 愛爾達 holds 「內銷」/「外銷」.
-- Every one of them can be picked up by fill_weight as a weight_item and quoted
-- into Core evidence, and no check could see it, because status and rows were
-- never required to agree.
--
-- Retraction is not deletion. The row, its source_file and its method stay, so
-- that what a past parse claimed remains auditable; what changes is that it is
-- no longer evidence. tw.product_revenue_share -- the view fill_weight, dossier
-- and the coverage checks consume -- excludes retracted rows, and excludes them
-- from the denominator too, so a retracted line cannot go on distorting the
-- shares of the lines that remain.

ALTER TABLE tw.product_revenue
    ADD COLUMN IF NOT EXISTS retracted_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retracted_reason TEXT;

ALTER TABLE tw.product_revenue
    DROP CONSTRAINT IF EXISTS product_revenue_retraction_reasoned;
ALTER TABLE tw.product_revenue
    ADD CONSTRAINT product_revenue_retraction_reasoned
    CHECK (retracted_at IS NULL OR btrim(COALESCE(retracted_reason, '')) <> '');

CREATE OR REPLACE VIEW tw.product_revenue_share AS
SELECT p.stock_id,
       p.fiscal_year,
       p.item,
       p.revenue,
       p.pct,
       COALESCE(p.pct,
           CASE WHEN t.total > 0::numeric
                THEN round(p.revenue * 100.0 / t.total, 4)
                ELSE NULL::numeric END) AS pct_effective,
       p.pct IS NULL AND p.revenue IS NOT NULL AS pct_is_derived,
       p.method,
       p.source_file
  FROM tw.product_revenue p
  LEFT JOIN (SELECT stock_id, fiscal_year, sum(revenue) AS total
               FROM tw.product_revenue
              WHERE revenue IS NOT NULL
                AND retracted_at IS NULL
              GROUP BY stock_id, fiscal_year) t
         ON t.stock_id::text = p.stock_id::text
        AND t.fiscal_year = p.fiscal_year
 WHERE p.retracted_at IS NULL;
