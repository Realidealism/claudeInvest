-- Retire the two-table classification the V3.7 schema replaced.
--
-- Everything these held now lives in tw.company_industry_exposure (448 rows,
-- 387 companies) and tw.company_theme_mapping (72 rows). The migration was
-- reconciled row by row before this ran: every one of the 452 old memberships
-- resolves to a row in the new tables, four pairs having collapsed because two
-- old themes name the same node, and the 304 old shares reconcile to 295 —
-- two lost to those collapses, eight deliberately dropped where the same
-- revenue line would otherwise have been counted on two nodes (R004), one
-- gained where a merge had picked the wrong line and was corrected.
--
-- The pre-drop state is at D:\invest_backup\themes_backup_20260901.json, and
-- db/themes_data.py.bak-20260901 / db/theme_tags_data.py.bak-20260901 hold the
-- source files that produced it.
DROP TABLE IF EXISTS tw.stock_themes;
DROP TABLE IF EXISTS tw.themes;
DROP TABLE IF EXISTS tw.stock_theme_tags;
DROP TABLE IF EXISTS tw.theme_tags;
