-- Volume status (洪量七級) for open positions, so the frontend 策略持倉 table
-- can show the same classification the signal factory gates on
-- (vol_strong = volume_status <= 2, see signal_backtest/factories/*.py).
--
-- Written by analysis/daily_snapshot.py and analysis/intraday_snapshot.py,
-- which already hold the classified StockData when they persist positions.
-- Codes: 0=洪 1=大量 2=量多 3=正常 4=量少 5=量縮 6=窒息 (analysis/volume.py).

ALTER TABLE tw.open_positions
    ADD COLUMN IF NOT EXISTS volume_status SMALLINT;

ALTER TABLE tw.open_positions_intraday
    ADD COLUMN IF NOT EXISTS volume_status SMALLINT;
