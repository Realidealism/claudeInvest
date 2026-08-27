-- The "被投資公司名稱、所在地區…等相關資訊" note covers BOTH consolidated
-- subsidiaries and equity-method associates, and carries richer figures than the
-- consolidated-subsidiary note alone. Widen the table to keep them.

ALTER TABLE tw.company_affiliations ADD COLUMN IF NOT EXISTS location TEXT;
ALTER TABLE tw.company_affiliations ADD COLUMN IF NOT EXISTS shares_held NUMERIC(20,0);
ALTER TABLE tw.company_affiliations ADD COLUMN IF NOT EXISTS book_value NUMERIC(20,0);
ALTER TABLE tw.company_affiliations ADD COLUMN IF NOT EXISTS investment_income NUMERIC(20,0);

-- consolidated now means "this pair appeared in the consolidated-subsidiary
-- note"; rows found only in the significant-influence note are FALSE.
ALTER TABLE tw.company_affiliations ALTER COLUMN consolidated SET DEFAULT FALSE;
