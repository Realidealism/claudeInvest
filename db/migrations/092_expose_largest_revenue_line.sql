-- Surface the largest reported line beside the classification, derived.
--
-- R023 says the Core follows the company's business essence, not its biggest
-- product line — 義隆's notebook input modules are 69.92% of revenue and it is
-- still a touch/HMI chip designer, because the module is that chip's carrier.
-- The rule only works if both facts are visible: hiding the larger line makes
-- the Core look arbitrary, and storing it beside the Core invites someone to
-- "fix" the mismatch.
--
-- So it is derived here, from the same annual report the weights come from, and
-- never written into the exposure row.
-- Dropped rather than replaced: the new columns sit in the middle, and
-- CREATE OR REPLACE can only append.
DROP VIEW IF EXISTS tw.v_company_industry_exposure;
CREATE VIEW tw.v_company_industry_exposure AS
WITH largest AS (
    SELECT DISTINCT ON (stock_id) stock_id, item, pct_effective
    FROM tw.product_revenue_share
    WHERE fiscal_year = 2025 AND pct_effective IS NOT NULL
      AND item NOT IN ('其他', '其它', '合計', '其他營業收入', '租賃收入', '勞務收入', '銷貨收入')
    ORDER BY stock_id, pct_effective DESC
)
SELECT e.id, e.stock_id, s.name AS company_name, s.industry AS official_industry,
       e.node_code, t.name AS node_name, t.level AS node_level,
       p.node_code AS parent_code, p.name AS parent_name,
       e.role, e.exposure_type,
       e.weight_pct, e.weight_type, e.weight_basis, e.weight_item,
       CASE
           WHEN e.weight_type IS DISTINCT FROM 'Revenue' THEN NULL
           WHEN e.weight_pct IS NULL THEN NULL
           WHEN e.weight_pct >= 50 THEN 'High'
           WHEN e.weight_pct >= 20 THEN 'Medium'
           ELSE 'Low'
       END AS revenue_exposure,
       l.item AS largest_revenue_item,
       l.pct_effective AS largest_revenue_pct,
       e.strategic_priority, e.strategic_evidence,
       e.evidence, e.evidence_kind, e.confidence,
       e.effective_from, e.effective_to, e.known_date, e.evidence_period_end
FROM tw.company_industry_exposure e
JOIN tw.stocks s ON s.stock_id = e.stock_id
JOIN tw.industry_tree t ON t.node_code = e.node_code
LEFT JOIN tw.industry_tree p ON p.node_code = t.parent_code
LEFT JOIN largest l ON l.stock_id = e.stock_id;
