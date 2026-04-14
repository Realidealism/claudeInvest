-- Add forming sort alignment counts to market breadth snapshot
ALTER TABLE tw.market_breadth
    ADD COLUMN IF NOT EXISTS short_up_forming   INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS short_down_forming  INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS medium_up_forming   INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS medium_down_forming INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS long_up_forming     INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS long_down_forming   INT NOT NULL DEFAULT 0;
