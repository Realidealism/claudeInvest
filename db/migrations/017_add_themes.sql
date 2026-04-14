-- Stock theme/concept tagging (題材/概念股標籤)

CREATE TABLE IF NOT EXISTS tw.themes (
    theme_id    SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL UNIQUE,   -- e.g. 'AI', '電動車'
    category    VARCHAR(30),                    -- grouping: '科技', '綠能', '傳產', '金融'
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tw.stock_themes (
    stock_id    VARCHAR(10) NOT NULL REFERENCES tw.stocks(stock_id),
    theme_id    INTEGER NOT NULL REFERENCES tw.themes(theme_id),
    note        TEXT,                           -- why this stock belongs to this theme
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, theme_id)
);

CREATE INDEX IF NOT EXISTS idx_tw_stock_themes_theme ON tw.stock_themes (theme_id);
