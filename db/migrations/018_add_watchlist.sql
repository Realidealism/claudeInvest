-- User watchlist persistence

CREATE TABLE IF NOT EXISTS tw.watchlist (
    id          SERIAL PRIMARY KEY,
    group_name  VARCHAR(50) NOT NULL DEFAULT '自選股',
    stock_id    VARCHAR(20) NOT NULL,
    sort_order  INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (group_name, stock_id)
);
