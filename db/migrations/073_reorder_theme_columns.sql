-- Rebuild tw.themes so the columns read top-down like the taxonomy itself:
-- category > subcategory > name (072 could only append subcategory at the end).
-- Guarded so re-running the file is a no-op once the order is already right.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'tw' AND table_name = 'themes'
          AND column_name = 'category' AND ordinal_position <> 2
    ) THEN
        CREATE TABLE tw.themes_reordered (
            theme_id    SERIAL PRIMARY KEY,
            category    VARCHAR(30),
            subcategory VARCHAR(30),
            name        VARCHAR(50) NOT NULL UNIQUE,
            description TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        );

        INSERT INTO tw.themes_reordered
            (theme_id, category, subcategory, name, description, created_at, updated_at)
        SELECT theme_id, category, subcategory, name, description, created_at, updated_at
        FROM tw.themes;

        PERFORM setval(
            pg_get_serial_sequence('tw.themes_reordered', 'theme_id'),
            GREATEST((SELECT COALESCE(MAX(theme_id), 0) FROM tw.themes_reordered), 1)
        );

        ALTER TABLE tw.stock_themes DROP CONSTRAINT stock_themes_theme_id_fkey;
        DROP TABLE tw.themes;
        ALTER TABLE tw.themes_reordered RENAME TO themes;
        ALTER TABLE tw.stock_themes
            ADD CONSTRAINT stock_themes_theme_id_fkey
            FOREIGN KEY (theme_id) REFERENCES tw.themes(theme_id);
        ALTER TABLE tw.themes RENAME CONSTRAINT themes_reordered_pkey TO themes_pkey;
        ALTER TABLE tw.themes RENAME CONSTRAINT themes_reordered_name_key TO themes_name_key;
    END IF;
END $$;
