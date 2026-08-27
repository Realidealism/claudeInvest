-- Some reports disclose only amounts and no percentages (慧洋: 租金收入
-- 16,657,969 with no ratio column), which left 28% of the rows invisible to any
-- consumer that filters on pct. The share is derivable from the amounts, so
-- expose it as a view rather than writing a second number into the table: the
-- stored pct stays exactly what the report printed.

CREATE OR REPLACE VIEW tw.product_revenue_share AS
SELECT
    p.stock_id,
    p.fiscal_year,
    p.item,
    p.revenue,
    p.pct,
    COALESCE(
        p.pct,
        CASE WHEN t.total > 0 THEN ROUND(p.revenue * 100.0 / t.total, 4) END
    ) AS pct_effective,
    (p.pct IS NULL AND p.revenue IS NOT NULL) AS pct_is_derived,
    p.method,
    p.source_file
FROM tw.product_revenue p
LEFT JOIN (
    SELECT stock_id, fiscal_year, SUM(revenue) AS total
    FROM tw.product_revenue
    WHERE revenue IS NOT NULL
    GROUP BY stock_id, fiscal_year
) t ON t.stock_id = p.stock_id AND t.fiscal_year = p.fiscal_year;
