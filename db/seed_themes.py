"""
Validating loader for stock themes (題材/概念股分類).

Canonical source is db/themes_data.py, which is human-edited by STOCK NAME.
Themes carry an optional middle level: category > subcategory > theme name.
A theme is identified by that full path, so the same name may appear under two
different branches (生產製程及檢測設備 exists under both 中游 and 下游).
This script resolves names -> stock_id against tw.stocks (active common stocks
only) and refuses to write anything if a single name fails to resolve.

    python -m db.seed_themes --check   # validate + print report, no DB write
    python -m db.seed_themes           # full sync: DB mirrors themes_data.py

Full sync means themes/mappings absent from themes_data.py are DELETED.
"""

import sys

from db.connection import get_cursor
from db.themes_data import THEMES, MAPPINGS


def _active_name_index(cur):
    """name -> [stock_id, ...] over active common stocks."""
    cur.execute("""
        SELECT stock_id, name FROM tw.stocks
        WHERE is_active = TRUE AND security_type = 'STOCK'
    """)
    index = {}
    for row in cur.fetchall():
        index.setdefault(row["name"], []).append(row["stock_id"])
    return index


def theme_path(name, category, subcategory):
    return " > ".join(x for x in (category, subcategory, name) if x)


def validate(cur):
    """Return (resolved, errors). resolved = {theme_path: [(name, stock_id), ...]}."""
    errors = []

    paths = [theme_path(n, c, sub) for n, c, sub, _d in THEMES]
    for path in sorted(set(paths)):
        if paths.count(path) > 1:
            errors.append(f"THEMES: duplicate theme path {path!r}")

    defined = set(paths)
    for path in MAPPINGS:
        if path not in defined:
            errors.append(f"MAPPINGS: theme path {path!r} is not defined in THEMES")

    index = _active_name_index(cur)
    resolved = {}
    for path, names in MAPPINGS.items():
        pairs = []
        seen = set()
        for name in names:
            if name in seen:
                errors.append(f"{path}: duplicate stock name {name!r}")
                continue
            seen.add(name)
            hits = index.get(name, [])
            if not hits:
                errors.append(f"{path}: unknown stock name {name!r} (delisted / ETF / typo?)")
            elif len(hits) > 1:
                errors.append(f"{path}: ambiguous stock name {name!r} -> {hits}")
            else:
                pairs.append((name, hits[0]))
        resolved[path] = pairs

    return resolved, errors


def report(resolved):
    total = 0
    for name, category, subcategory, _desc in THEMES:
        path = theme_path(name, category, subcategory)
        pairs = resolved.get(path, [])
        total += len(pairs)
        print(f"{chr(10)}[{path}]  ({len(pairs)})")
        if not pairs:
            print("  (no stocks)")
        for stock_name, stock_id in pairs:
            print(f"  {stock_id}  {stock_name}")
    print(f"{chr(10)}{len(THEMES)} themes, {total} mappings.")


def sync(cur, resolved):
    """Make DB an exact mirror of themes_data.py."""
    for name, category, subcategory, description in THEMES:
        cur.execute("""
            INSERT INTO tw.themes (name, category, subcategory, description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (category, subcategory, name) DO UPDATE SET
                description = EXCLUDED.description,
                updated_at = NOW()
        """, (name, category, subcategory, description))

    cur.execute("SELECT theme_id, category, subcategory, name FROM tw.themes")
    rows = cur.fetchall()
    theme_id_of = {
        theme_path(r["name"], r["category"], r["subcategory"]): r["theme_id"] for r in rows
    }
    keep = {theme_path(n, c, sub) for n, c, sub, _d in THEMES}
    stale = [tid for path, tid in theme_id_of.items() if path not in keep]
    if stale:
        cur.execute("DELETE FROM tw.stock_themes WHERE theme_id = ANY(%s)", (stale,))
        cur.execute("DELETE FROM tw.themes WHERE theme_id = ANY(%s)", (stale,))
        print(f"Removed {len(stale)} themes not present in themes_data.py")

    added = removed = 0
    for path, pairs in resolved.items():
        theme_id = theme_id_of[path]
        wanted = {stock_id for _name, stock_id in pairs}
        cur.execute("SELECT stock_id FROM tw.stock_themes WHERE theme_id = %s", (theme_id,))
        current = {r["stock_id"] for r in cur.fetchall()}

        for stock_id in sorted(wanted - current):
            cur.execute(
                "INSERT INTO tw.stock_themes (stock_id, theme_id) VALUES (%s, %s)",
                (stock_id, theme_id),
            )
            added += 1
        drop = sorted(current - wanted)
        if drop:
            cur.execute(
                "DELETE FROM tw.stock_themes WHERE theme_id = %s AND stock_id = ANY(%s)",
                (theme_id, drop),
            )
            removed += len(drop)

    cur.execute("SELECT COUNT(*) AS cnt FROM tw.themes")
    theme_count = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) AS cnt FROM tw.stock_themes")
    mapping_count = cur.fetchone()["cnt"]
    print(f"Synced: +{added} / -{removed} mappings. "
          f"DB now holds {theme_count} themes, {mapping_count} mappings.")


def main():
    check_only = "--check" in sys.argv[1:]
    unknown = [a for a in sys.argv[1:] if a != "--check"]
    if unknown:
        sys.exit(f"Unknown argument(s): {unknown}. Use --check or no argument.")

    with get_cursor() as cur:
        resolved, errors = validate(cur)
        report(resolved)
        if errors:
            print(f"\n{len(errors)} problem(s) — nothing written:")
            for e in errors:
                print(f"  ! {e}")
            sys.exit(1)
        if check_only:
            print("\n--check: validation passed, no DB write.")
            return
        sync(cur, resolved)


if __name__ == "__main__":
    main()
