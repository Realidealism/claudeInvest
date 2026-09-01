"""Load the classification skeleton: the tree, the theme dictionary, the
reference dictionaries, and the exchange-category bridge.

These four move together and almost never move at all, which is why they share
one loader. Membership is a different thing entirely and lives in
db.seed_industry / db.seed_themes.

The tree and the theme dictionary are mirrored from their data files: a node
absent from the file is deleted here, so a node that still has members cannot
be removed silently — the foreign key from tw.company_industry_exposure stops
the delete and the run aborts. That is the intended behaviour. Retiring a node
means moving its members first.

The reference dictionaries come from db/reference_data.py — static arrays with
nothing to maintain by hand, transcribed once so that nothing here has to reach
outside the repository for data.

    python -m db.seed_skeleton --check    # report only, writes nothing
    python -m db.seed_skeleton            # sync
"""

import argparse

from db.connection import get_cursor
from db.industry_tree_data import NODES
from db.theme_master_data import THEMES
from db.reference_data import (
    SUPPLY_CHAIN_POSITIONS, SUPPLY_CHAIN_LAYERS, CYCLE_ATTRIBUTES,
    REGION_EXPOSURE_RULES, MANUFACTURING_MODES, TWSE_INDUSTRY_MAPPING, V37_RULES,
)

# (table, columns, rows) — keyed on the first column
REFERENCE_DICTS = [
    ("tw.supply_chain_position", ("position_name", "seq", "description"),
     SUPPLY_CHAIN_POSITIONS),
    ("tw.supply_chain_layer", ("layer", "coverage"), SUPPLY_CHAIN_LAYERS),
    ("tw.cycle_attributes", ("attribute", "representative_industries"), CYCLE_ATTRIBUTES),
    ("tw.classification_rules", ("rule_id", "rule", "description"), V37_RULES),
    ("tw.twse_industry_mapping",
     ("twse_official_category", "master_l1", "master_l2_range", "mapping_type"),
     TWSE_INDUSTRY_MAPPING),
]


# Rules we authored ourselves, applied on top of the V3.7 draft's R001-R021.
# They live here rather than in the CSV because the CSV is the imported draft
# and should stay recognisably what was handed to us.
LOCAL_RULES = [
    ("R022", "通路/代理商的產品線不得據以認定製造歸屬",
     "若公司為通路/代理商，其產品線中出現設備、材料、載板、IC 等品項時，"
     "不得僅因產品名稱將其掛到對應的製造節點；該產品最多作為 Indirect 曝險。"
     "否則產業統計會把通路商錯算成製造商——實例：港建(設備)、利機(BT載板)、"
     "陞泰(電子材料)、蔚華科(測試設備)。判斷依據優先序為官方分類、"
     "年報是否有佣金收入等代理特徵、以及年報正文對自有產能的描述。"),
]


def validate():
    """Structural checks on the tree; raises rather than writing a broken tree."""
    codes = {n[0] for n in NODES}
    problems = []
    for code, parent, level, name, _products in NODES:
        if parent and parent not in codes:
            problems.append(f"{code}: parent {parent!r} is not a node")
        if (parent is None) != (level == 1):
            problems.append(f"{code}: level {level} with parent {parent!r}")
        if not name.strip():
            problems.append(f"{code}: empty name")
    if len(codes) != len(NODES):
        problems.append("duplicate node_code in NODES")
    theme_codes = {t[0] for t in THEMES}
    if len(theme_codes) != len(THEMES):
        problems.append("duplicate theme_code in THEMES")
    return problems


def sync_tree(cur):
    # Parents before children, so the self-referencing key is satisfied as we go.
    for node in sorted(NODES, key=lambda n: n[2]):
        cur.execute("""
            INSERT INTO tw.industry_tree
                   (node_code, parent_code, level, name, main_products)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (node_code) DO UPDATE SET
                parent_code = EXCLUDED.parent_code,
                level = EXCLUDED.level,
                name = EXCLUDED.name,
                main_products = EXCLUDED.main_products,
                updated_at = NOW()
        """, node)

    cur.execute("SELECT node_code FROM tw.industry_tree")
    stale = [r["node_code"] for r in cur.fetchall() if r["node_code"] not in {n[0] for n in NODES}]
    if stale:
        # Deepest first: an L2 cannot go before its children.
        cur.execute("DELETE FROM tw.industry_tree WHERE node_code = ANY(%s)", (stale,))
    return len(NODES), len(stale)


def sync_themes(cur):
    for theme in THEMES:
        cur.execute("""
            INSERT INTO tw.theme_master
                   (theme_code, theme_group, theme_name, sub_theme, related_industries)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (theme_code) DO UPDATE SET
                theme_group = EXCLUDED.theme_group,
                theme_name = EXCLUDED.theme_name,
                sub_theme = EXCLUDED.sub_theme,
                related_industries = EXCLUDED.related_industries
        """, theme)

    cur.execute("SELECT theme_code FROM tw.theme_master")
    stale = [r["theme_code"] for r in cur.fetchall() if r["theme_code"] not in {t[0] for t in THEMES}]
    if stale:
        cur.execute("DELETE FROM tw.theme_master WHERE theme_code = ANY(%s)", (stale,))
    return len(THEMES), len(stale)


def sync_reference_dicts(cur):
    counts = {}
    for table, db_cols, rows in REFERENCE_DICTS:
        key = db_cols[0]
        placeholders = ", ".join(["%s"] * len(db_cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in db_cols[1:])
        for row in rows:
            cur.execute(
                f"INSERT INTO {table} ({', '.join(db_cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT ({key}) DO UPDATE SET {updates}",
                row,
            )
        counts[table] = len(rows)

    for rule_id, rule, description in LOCAL_RULES:
        cur.execute("""
            INSERT INTO tw.classification_rules (rule_id, rule, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (rule_id) DO UPDATE SET
                rule = EXCLUDED.rule, description = EXCLUDED.description
        """, (rule_id, rule, description))
    counts["tw.classification_rules"] += len(LOCAL_RULES)

    # region_exposure_rules and manufacturing_mode_examples have no natural key,
    # so they are replaced wholesale rather than upserted.
    cur.execute("DELETE FROM tw.region_exposure_rules")
    for row in REGION_EXPOSURE_RULES:
        cur.execute(
            "INSERT INTO tw.region_exposure_rules (section, item, value) VALUES (%s, %s, %s)",
            row)
    cur.execute("DELETE FROM tw.manufacturing_mode_examples")
    for row in MANUFACTURING_MODES:
        cur.execute(
            "INSERT INTO tw.manufacturing_mode_examples (company, manufacturing_mode, product) "
            "VALUES (%s, %s, %s)", row)
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="validate and report, write nothing")
    args = ap.parse_args()

    problems = validate()
    if problems:
        print("VALIDATION FAILED")
        for p in problems:
            print("  -", p)
        raise SystemExit(1)

    levels = {lv: sum(1 for n in NODES if n[2] == lv) for lv in (1, 2, 3)}
    print(f"tree: {len(NODES)} nodes (L1={levels[1]} L2={levels[2]} L3={levels[3]})")
    print(f"themes: {len(THEMES)}")
    if args.check:
        print("--check: nothing written")
        return

    with get_cursor() as cur:
        n_tree, stale_tree = sync_tree(cur)
        n_theme, stale_theme = sync_themes(cur)
        counts = sync_reference_dicts(cur)

    print(f"industry_tree: {n_tree} upserted, {stale_tree} removed")
    print(f"theme_master: {n_theme} upserted, {stale_theme} removed")
    for table, n in counts.items():
        print(f"{table}: {n}")


if __name__ == "__main__":
    main()
