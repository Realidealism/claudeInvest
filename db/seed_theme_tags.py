"""Sync tw.theme_tags / tw.stock_theme_tags to match db/theme_tags_data.py.

Same contract as db.seed_themes: resolve 股名 against tw.stocks first and
refuse to write anything if a single name fails, so a typo can never land a
half-applied set. The DB is a full mirror of the file — tags and memberships
not listed there are removed.

    python -m db.seed_theme_tags --check   # validate and report, no DB write
    python -m db.seed_theme_tags           # sync
"""
import sys

from db.connection import get_cursor
from db.theme_tags_data import TAGS, MEMBERS

LEVELS = ("core", "peripheral", "speculative")
EVIDENCE_KINDS = ("annual_report_prose", "investor_conf", "company_site", "news")


def _active_name_index(cur):
    cur.execute(
        "SELECT stock_id, name FROM tw.stocks "
        "WHERE is_active AND security_type = 'STOCK'")
    index = {}
    for row in cur.fetchall():
        index.setdefault(row["name"], []).append(row["stock_id"])
    return index


def validate(cur):
    """Return (resolved, errors); resolved is {tag_path: [(stock_id, 股名, ...)]}."""
    index = _active_name_index(cur)
    declared = {f"{category} > {name}" for name, category, _ in TAGS}
    errors = []

    for path in MEMBERS:
        if path not in declared:
            errors.append(f"{path}: members listed for a tag not in TAGS")
    for path in declared:
        if path not in MEMBERS:
            errors.append(f"{path}: tag declared with no members")

    resolved = {}
    for path, rows in MEMBERS.items():
        seen = set()
        out = []
        for stock_name, level, kind, evidence in rows:
            if stock_name in seen:
                errors.append(f"{path}: {stock_name} listed twice")
                continue
            seen.add(stock_name)
            if level not in LEVELS:
                errors.append(f"{path} / {stock_name}: level {level!r} not in {LEVELS}")
            if kind not in EVIDENCE_KINDS:
                errors.append(f"{path} / {stock_name}: evidence_kind {kind!r} unknown")
            if not evidence.strip():
                errors.append(f"{path} / {stock_name}: evidence is required")
            ids = index.get(stock_name)
            if not ids:
                errors.append(f"{path}: unknown stock name {stock_name!r}")
            elif len(ids) > 1:
                errors.append(f"{path}: ambiguous stock name {stock_name!r} -> {ids}")
            else:
                out.append((ids[0], stock_name, level, kind, evidence))
        resolved[path] = out
    return resolved, errors


def report(resolved):
    for path, rows in resolved.items():
        by_level = {lv: [r[1] for r in rows if r[2] == lv] for lv in LEVELS}
        print(f"\n[{path}]  ({len(rows)})")
        for lv in LEVELS:
            if by_level[lv]:
                print(f"  {lv:12s} {'、'.join(by_level[lv])}")
    total = sum(len(r) for r in resolved.values())
    print(f"\n{len(TAGS)} tags, {total} memberships.")


def sync(cur, resolved):
    cur.execute("SELECT tag_id, category, name FROM tw.theme_tags")
    existing = {(r["category"], r["name"]): r["tag_id"] for r in cur.fetchall()}

    tag_ids = {}
    for name, category, description in TAGS:
        cur.execute(
            "INSERT INTO tw.theme_tags (category, name, description) VALUES (%s, %s, %s) "
            "ON CONFLICT (category, name) DO UPDATE "
            "SET description = EXCLUDED.description, updated_at = NOW() "
            "RETURNING tag_id",
            (category, name, description))
        tag_ids[f"{category} > {name}"] = cur.fetchone()["tag_id"]

    stale = [tid for key, tid in existing.items()
             if f"{key[0]} > {key[1]}" not in tag_ids]
    if stale:
        cur.execute("DELETE FROM tw.stock_theme_tags WHERE tag_id = ANY(%s)", (stale,))
        cur.execute("DELETE FROM tw.theme_tags WHERE tag_id = ANY(%s)", (stale,))
        print(f"Removed {len(stale)} tags not present in theme_tags_data.py")

    added = removed = 0
    for path, rows in resolved.items():
        tag_id = tag_ids[path]
        cur.execute(
            "SELECT stock_id FROM tw.stock_theme_tags WHERE tag_id = %s", (tag_id,))
        current = {r["stock_id"] for r in cur.fetchall()}
        wanted = {r[0] for r in rows}

        gone = current - wanted
        if gone:
            cur.execute(
                "DELETE FROM tw.stock_theme_tags WHERE tag_id = %s AND stock_id = ANY(%s)",
                (tag_id, list(gone)))
            removed += len(gone)

        # Levels and evidence get rewritten on every sync: the file is the
        # source of truth for them, not just for who is a member.
        for stock_id, _, level, kind, evidence in rows:
            cur.execute(
                "INSERT INTO tw.stock_theme_tags "
                "(stock_id, tag_id, level, evidence, evidence_kind) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (stock_id, tag_id) DO UPDATE "
                "SET level = EXCLUDED.level, evidence = EXCLUDED.evidence, "
                "    evidence_kind = EXCLUDED.evidence_kind",
                (stock_id, tag_id, level, evidence, kind))
            if stock_id not in current:
                added += 1

    cur.execute("SELECT count(*) AS n FROM tw.stock_theme_tags")
    total = cur.fetchone()["n"]
    print(f"Synced: +{added} / -{removed} memberships. "
          f"DB now holds {len(tag_ids)} tags, {total} memberships.")


def main():
    check_only = "--check" in sys.argv[1:]
    unknown = [a for a in sys.argv[1:] if a != "--check"]
    if unknown:
        sys.exit(f"Unknown argument(s): {unknown}. Use --check or no argument.")

    with get_cursor() as cur:
        resolved, errors = validate(cur)
        if errors:
            for e in errors:
                print("ERROR:", e)
            sys.exit(f"{len(errors)} error(s); nothing was written.")
        report(resolved)
        if check_only:
            print("\n--check: validation passed, no DB write.")
            return
        sync(cur, resolved)


if __name__ == "__main__":
    main()
