-- Track which conference decks have been pulled to local storage, so the daily
-- step can fetch only what is new instead of re-downloading the whole index.

ALTER TABLE tw.investor_conferences ADD COLUMN IF NOT EXISTS local_path TEXT;
ALTER TABLE tw.investor_conferences ADD COLUMN IF NOT EXISTS downloaded_at TIMESTAMPTZ;
