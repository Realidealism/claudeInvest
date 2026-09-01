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
    ("R006", "各權重維度須標明來源與計算方式，不得混用",
     "產業曝險可使用 Revenue／Profit／Capacity／Strategic Priority 等不同權重維度；"
     "各維度須明確標示來源與計算方式，不得混用。"
     "目前唯一有資料來源的是 Revenue（年報營業比重表，分母為合併營收）；"
     "Profit 因台灣年報普遍不揭露分部損益而無來源，欄位已移除而非留白待猜（見 R020）。"),

    # R004 as drafted read as "one industry per company" to more than one
    # reader. The constraint is on the business line, not the company.
    ("R004", "同一業務曝險僅能有一個 Industry 歸屬",
     "公司可同時存在多個 Industry Exposure；不同業務線可分別歸屬不同 Industry 節點，"
     "但同一業務線不得重複計入多個節點。例：欣興的 ABF 載板歸 B3-01、硬板歸 B2-01，兩者並存；"
     "但同一條年報營收線不得同時在兩個節點計入比重（見 R020 與 R022）。"),

    # "新產品" was too wide: a product can be new and still have a home.
    ("R005", "尚未有適當 Industry 節點的新興技術/產品優先進入 Theme",
     "若已有適當 Industry 節點，Industry 與 Theme 可並存，不必二選一。"
     "HBM 即為此例：Theme T07 與 A3 Memory／A5 HBM Packaging／A6 HBM Test 同時成立（見 R017）。"),

    ("R015", "A3 兩維度：製造模式 × 產品類型",
     "Foundry/IDM＝製造模式（商業模式）；Memory/Power/CIS/Compound＝產品類型。"
     "★兩者可同時存在、不互斥：一家 IDM 可同時掛 Power 與 Compound 產品節點；"
     "Foundry 不是產品分類，不可被讀成 A3 的一個產品格。"),

    # Theme leaves the exposure type: it has its own table and its own contract.
    ("R018", "Exposure Type 只有 Direct / Indirect；Confidence 另計",
     "Type：Direct（自有業務）／Indirect（該營收已被他處計入，見 R020、R022）。"
     "Theme 不是 exposure type——題材有獨立表與獨立契約（無營收判準、evidence 必填），"
     "若容許 exposure_type='Theme' 會與 R003「Theme 不可作為 Primary Industry」自相矛盾。"
     "★evidence_kind 記錄**證據類型**，confidence 記錄**對該 Exposure 判斷的可信程度**，"
     "兩者不得視為同義欄位、也不得由前者機械映射後者："
     "欣興「印刷電路板 99.07%」是品質極高的財報證據，卻無法支持任何單一節點的配置——"
     "證據品質高、節點歸屬信心不可得，兩件事同時成立。"),

    ("R019", "Core Industry 由曝險表推導，不手動維護",
     "推導依「業務群合併後之主要經濟曝險」判定，**不是**逕取單一營收品項最大值。"
     "例：精材測試服務 47.86% 大於晶圓級尺寸封裝 44.98%，但後者與後護層封裝 5.18% 同屬封裝業務群"
     "合計 50.16%，故 Core 為 A5-02 而非 A6-01。"
     "★若無法唯一判定，Core 可為 NULL（待確認），載入器不因此拒收，由 db.check_industry 列出。"
     "★Core ≠ Revenue Exposure Level：超豐無比重仍為 Core，精材 44.98% 的 revenue_exposure 是 Medium "
     "但仍是 Core。前者是產業定位，後者是營收分箱。★業務群本身的排序見 R023。"),

    ("R020", "Weight 可為 NULL，且必須標明類型與分母",
     "Revenue／Profit／Capacity／Strategic 分開記錄，須標示 weight_type 與 weight_basis（分母）；"
     "無此二者的裸數字不可比較——47.86% 是營收占比、30% 可能是產能或策略描述。"
     "未揭露時 Weight=NULL，不得臆測百分比。"
     "同一條營收線在第二個節點出現時該列 weight 必為 NULL 且 exposure_type='Indirect'。"
     "★weight_pct 是**該節點的合計**而非單一年報線：一個節點可擁有公司的多條線，"
     "weight_item 以 \" + \" 列出組成線（創意 A4-05＝委託設計 23.53%＋晶圓產品 75.38%＝98.91%）；"
     "任一組成線解不出來則整筆留 NULL，不給部分和。"
     "★Revenue Exposure 等級不落地儲存，由 weight 經 view tw.v_company_industry_exposure "
     "機械推導（<20% Low／20-50% Medium／>=50% High）；非 Revenue 型權重不產生等級。"),

    ("R021", "時間三軸：Effective / Known / Evidence",
     "Effective_From/To＝這個分類何時成立；Known_Date＝我們最早何時可能知道（MOPS 申報日）；"
     "Evidence_Period_End＝證據本身對應哪個期間（FY2025 年報＝2025-12-31）。三者不可互相代替。"
     "回測一律以 Known_Date 過濾以避免 look-ahead；Effective_To 保持 NULL 表示仍為現行層，"
     "不可拿會計年度結束日去填，否則現行層會整批失效。"),

    ("R023", "Core 依商業本質與價值鏈位置判定，不以單一營收產品名稱決定",
     "營收最大的產品線不等於公司的產業歸屬；最大營收品項另存於曝險列，Core 另行判定。"
     "義隆的筆電輸入裝置模組 69.92% 大於其觸控 IC 27.72%，但它是觸控/HMI 晶片設計公司，"
     "模組是該晶片的下游載體——若逕以最大營收線決定 Core，一家 IC 設計公司會被歸成週邊模組廠，"
     "產業本質就錯了。與 R019 併用：R019 說 Core 要看業務群合併而非單一線，R023 再加一層，"
     "業務群本身也要以價值鏈位置而非營收大小排序。"),

    ("R024", "IC 設計與模組/系統整合可並存，下游模組不得自動奪走 Core",
     "公司同時從事 IC 設計與模組/系統整合時，兩者可各自建立曝險；"
     "不得因下游模組營收較高而自動把 Core 改為模組產業。"
     "義隆＝Core A4-15 感測/HMI IC ＋ Major C11-01 筆電輸入裝置模組即為此形。"
     "★此規則不適用於本業就是製造/封裝的公司：同欣電的封裝業務大於陶瓷基板，兩者同為其製造本業，"
     "Core 隨業務群移動是對的。"),

    ("R022", "通路/代理商的產品線不得據以認定製造歸屬",
     "若公司為通路/代理商，其產品線中出現設備、材料、載板、IC 等品項時，不得僅因產品名稱"
     "將其掛到對應的製造節點；該產品最多作為 Indirect Industry Exposure，"
     "**不得因此建立製造型 Core**。例：利機 V1 半導體通路 Direct + BT 載板 Indirect；"
     "港建 V1 Direct + 設備 Indirect。否則產業統計會把通路商錯算成製造商。"
     "判斷依據以年報為主：是否有佣金/代理性收入、產品線是否為具名製造品、正文有無自有產能描述。"
     "★官方分類僅作後驗參考不居優先（R002）——長華* 官方列電子通路業，但年報是 IC導線架 48.91% "
     "加封膠樹酯 29.29% 且無佣金收入，實為製造商；若以官方分類為首要依據會把它誤判成通路。"),
]

# 22 rules read as a list; grouped they read as a structure.
RULE_CATEGORIES = {
    "產業分類": ["R001", "R002", "R004", "R011", "R015", "R016", "R019",
             "R022", "R023", "R024"],
    "題材": ["R003", "R005", "R010", "R012", "R017"],
    "權重與證據": ["R006", "R018", "R020"],
    "供應鏈": ["R007"],
    "集團與生態系": ["R013", "R014"],
    "公司屬性": ["R008", "R009"],
    "時間": ["R021"],
}


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

    rule_category = {rid: cat for cat, ids in RULE_CATEGORIES.items() for rid in ids}
    for rule_id, rule, description in LOCAL_RULES:
        cur.execute("""
            INSERT INTO tw.classification_rules (rule_id, rule, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (rule_id) DO UPDATE SET
                rule = EXCLUDED.rule, description = EXCLUDED.description
        """, (rule_id, rule, description))
    counts["tw.classification_rules"] += sum(
        1 for r in LOCAL_RULES if r[0] not in {x[0] for x in V37_RULES})

    for rule_id, category in rule_category.items():
        cur.execute("UPDATE tw.classification_rules SET category = %s WHERE rule_id = %s",
                    (category, rule_id))

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
