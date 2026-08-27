-- Revenue share of the theme's business inside the company, so a 0.4% side line
-- (柏騰's 碳化矽) is distinguishable from a 100% pure play (台積電's 晶圓).
-- Filled from tw.product_revenue by matching the annual report's product wording
-- against the theme; NULL means "not matched", never "zero".

ALTER TABLE tw.stock_themes ADD COLUMN IF NOT EXISTS revenue_pct NUMERIC(7,4);
ALTER TABLE tw.stock_themes ADD COLUMN IF NOT EXISTS pct_item TEXT;
ALTER TABLE tw.stock_themes ADD COLUMN IF NOT EXISTS pct_source TEXT;
