-- Themes get an optional middle level between category and name
-- (e.g. 半導體上游 > IC設計 > 記憶體控制IC). NULL for themes that sit
-- directly under their category.

ALTER TABLE tw.themes ADD COLUMN IF NOT EXISTS subcategory VARCHAR(30);
