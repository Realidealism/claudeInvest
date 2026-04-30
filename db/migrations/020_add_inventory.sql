-- Broker inventory snapshot (庫存快照)

CREATE TABLE IF NOT EXISTS tw.inventory (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20) NOT NULL,
    symbol_name         VARCHAR(50),
    current_quantity    INT NOT NULL DEFAULT 0,
    average_price       NUMERIC(12,4),
    current_price       NUMERIC(12,4),
    cost                NUMERIC(14,2),
    market_value        NUMERIC(14,2),
    unrealized_pnl      NUMERIC(14,2),
    unrealized_pnl_rate NUMERIC(8,4),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (symbol)
);
