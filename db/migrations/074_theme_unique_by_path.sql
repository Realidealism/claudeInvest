-- A theme name is only unique within its (category, subcategory) branch:
-- the source taxonomy reuses names across levels, e.g. 生產製程及檢測設備
-- exists under both 半導體中游 and 半導體下游. NULLS NOT DISTINCT so that a
-- NULL subcategory still collides with another NULL one (PG 15+).

ALTER TABLE tw.themes DROP CONSTRAINT IF EXISTS themes_name_key;
ALTER TABLE tw.themes DROP CONSTRAINT IF EXISTS themes_path_key;
ALTER TABLE tw.themes
    ADD CONSTRAINT themes_path_key
    UNIQUE NULLS NOT DISTINCT (category, subcategory, name);
