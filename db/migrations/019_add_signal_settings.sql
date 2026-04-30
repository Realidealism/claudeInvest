-- Persistent signal/indicator settings for chart UI

CREATE TABLE IF NOT EXISTS tw.signal_settings (
    key         VARCHAR(50) PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
