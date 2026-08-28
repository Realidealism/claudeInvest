-- Which kind of evidence put a company into a supply-chain theme.
--
-- Until now every membership was justified the same way: the product-line mix
-- in the annual report (tw.product_revenue), checked by db/check_themes.py.
-- That works for most of the chain but not for a few材料 corners, because the
-- annual report's 營業比重 table is written at whatever granularity the company
-- chose: 新應材 books 100% as 「精密特用化學材料」 and 光洋科 books 83.58% as
-- 「貴金屬材料」, so a search for 光阻 or 靶材 over every 2025 filing returns
-- zero rows even though both firms are pure plays in those corners.
--
-- Those themes are still worth having, so they fall back to weaker sources
-- (the report's 營運概況 chapter, an investor-conference deck, the company's
-- own product pages). Recording which source was used keeps the distinction
-- visible: a reader can tell a revenue-table membership from a prose one, and
-- a later cleanup can re-check only the soft ones.
--
-- Values: 'annual_report_table' | 'annual_report_prose' | 'investor_conf'
--       | 'company_site'
-- NULL means the membership predates this column.

ALTER TABLE tw.stock_themes
    ADD COLUMN IF NOT EXISTS evidence_kind TEXT;
