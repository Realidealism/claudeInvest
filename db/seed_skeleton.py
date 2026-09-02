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
     "Profit 因台灣年報普遍不揭露分部損益而無來源，欄位已移除而非留白待猜（見 R020）。"
     "★現況限制：db.fill_weight 每次執行會清空全表的 weight_pct/weight_type/weight_basis 再回填 "
     "Revenue 型，所以目前只有 Revenue 型權重能留在庫裡；要落地 Capacity/Strategic 型，"
     "必須先讓該清空語句只作用於 weight_type='Revenue'。"),

    # R004 as drafted read as "one industry per company" to more than one
    # reader. The constraint is on the business line, not the company.
    ("R004", "同一業務曝險僅能有一個 Industry 歸屬",
     "公司可同時存在多個 Industry Exposure；不同業務線可分別歸屬不同 Industry 節點，"
     "但同一業務線不得重複計入多個節點。例：欣興的 ABF 載板歸 B3-01、硬板歸 B2-01，兩者並存；"
     "但同一條年報營收線不得同時在兩個節點計入比重（見 R020 與 R022）。"),

    # "新產品" was too wide: a product can be new and still have a home.
    ("R005", "尚未有適當 Industry 節點的新興技術/產品優先進入 Theme",
     "若已有適當 Industry 節點，Industry 與 Theme 可並存，不必二選一。"
     "HBM 即為此例：Theme T07 與 A3-03 DRAM 製造／A5-04 HBM Packaging／A6-07 HBM Test "
     "同時成立（見 R017）。★節點一律引用 node_code，不引用「A3 Memory」這類非節點的口語名——"
     "A3 底下沒有 Memory 格，只有 A3-03 DRAM 製造與 A3-07 NOR/NAND Flash 製造。"),

    ("R015", "A3 兩維度：製造模式 × 產品類型",
     "Foundry/IDM＝製造模式（商業模式）；Memory/Power/CIS/Compound＝產品類型。"
     "★兩者可同時存在、不互斥：一家 IDM 可同時掛 Power 與 Compound 產品節點；"
     "Foundry 不是產品分類，不可被讀成 A3 的一個產品格。"),

    # Theme leaves the exposure type: it has its own table and its own contract.
    ("R018", "Exposure Type 只有 Direct / Indirect；Confidence 另計",
     "Type：Direct＝該公司自行創造之產業業務；Indirect＝該產業曝險在經濟歸屬上不應視為"
     "該節點之直接製造／業務曝險，包括代理／通路性質（見 R022），以及已由同一 Industry node "
     "之合併子公司獨立計入而會造成市場層級重複計數者（見 R020③）。"
     "★不可簡化成「該營收已被他處計入」——代理商的營收是它自己的帳，並沒有被別人計過，"
     "間接的是它與該製造節點的關係。"
     "Theme 不是 exposure type——題材有獨立表與獨立契約（無營收判準、evidence 必填），"
     "若容許 exposure_type='Theme' 會與 R003「Theme 不可作為 Primary Industry」自相矛盾。"
     "★evidence_kind 記錄**證據類型**，confidence 記錄**對該 Exposure 判斷的可信程度**，"
     "兩者不得視為同義欄位、也不得由前者機械映射後者："
     "欣興「印刷電路板 99.07%」是品質極高的財報證據，卻無法支持任何單一節點的配置——"
     "證據品質高、節點歸屬信心不可得，兩件事同時成立。"
     "★定義：confidence＝對**該 Industry Exposure 歸屬判斷本身**的信心，"
     "不是對來源文件可信度的評價（後者是 evidence_kind 的事）。"),

    ("R019", "Core Industry 由產業曝險資料推導，不手動維護",
     "Core **不由單一 weight_pct 最大值機械決定**，而是以產業曝險表為基礎，"
     "先將屬於同一 Industry node 的業務線聚合，再依聚合後之主要經濟曝險、價值創造能力、"
     "價值鏈角色與公司產業定位判定。"
     "★只有確實屬於同一 Industry node 的業務線才能聚合，**不得因上層產業、供應鏈或終端應用相近"
     "而跨節點合併**，亦不得跨節點為取得較大比重而合併："
     "A5 封裝 45% 與 A6 測試 48% 不可併成「半導體 93%」再據以定 Core，那等於把 taxonomy 拆掉；"
     "中石化的己內醯胺/尼龍粒 50.16%（J9）與丙烯腈 49.45%（J1-01）分屬兩節點，"
     "不得合併成「石化 99%」；本例依公司主要產業定位判定 Core 為 J9，"
     "而非因兩者合計後的 99% 得出。"
     "例：精材測試服務 47.86% 大於晶圓級尺寸封裝 44.98%，但後者與後護層封裝 5.18% 同屬封裝業務群"
     "合計 50.16%，故 Core 為 A5-02 而非 A6-01。"
     "★若無法唯一判定，該公司**不建立 Core 列**（role 是列上的 enum，沒有 NULL 這個值），"
     "載入器不因此拒收，由 db.check_industry 的 [core] 檢查列出待確認。"
     "★此例的節點合計必須真的落地（R020④）：A5-02 的 weight_item 是"
     "「晶圓級尺寸封裝 + 晶圓級後護層封裝」、weight_pct=50.16——若只存 44.98，"
     "資料本身會顯示 Core 小於 Major，規則的論證在庫裡就是假的。"
     "★Core ≠ Revenue Exposure Level：超豐無比重仍為 Core。前者是產業定位，後者是營收分箱。"),

    ("R020", "Weight 可為 NULL，且必須標明類型與分母",
     "① weight_pct 表示該 Industry 節點可歸屬之營收曝險，須標示 weight_type／weight_basis，"
     "不得將不同分母或不同層級的數字混用——47.86% 是營收占比、30% 可能是產能或策略描述，"
     "無此二者的裸數字不可比較；未揭露時 weight_pct=NULL，不得臆測百分比。"
     "② 同一營收線不得在多個 Industry 節點重複計重。若單一年報營收線涵蓋多個節點但無法可靠拆分，"
     "相關節點仍可建立 Direct Exposure，但各節點 weight_pct=NULL，並以 "
     "evidence_kind='filing_too_coarse' 說明；不得將該上層營收線比重套用至任一子節點。"
     "例：欣興「印刷電路板 99.07%」橫跨 B3-01 ABF 載板與 B2-01 硬板、"
     "德律「自動檢測設備 97.21%」橫跨 A2-13 測試/檢測設備（半導體側）與 B5-01 製程設備（PCBA 側），"
     "兩對都是兩格 Direct、兩格 NULL——**包括原本掛著比重的那一格**；"
     "台化「化工一部 39%」同理，部門名稱不指明產品，不可當成任一細格的比重。"
     "③ 若母公司合併營收線之產業曝險包含已於同一 Industry node 獨立建檔之合併子公司，"
     "且該營收已由該子公司在市場層級獨立計入，母公司對應曝險應**依是否產生重複計數判定為 "
     "Indirect**；不得因母公司合併營收再次將同一經濟活動計入市場層級產業權重。"
     "★判準是會不會重複計數，不是子公司佔多少——寫「主要來自」會把門檻變成爭論點。"
     "該 Indirect 列的 weight_pct 一律 NULL，比重僅保留於 evidence——年報確實揭露了這個數字，"
     "不寫明就會有人依 ① 把它填進去，重複計數就從這裡進來。"
     "例：茂達離散式功率元件 44.91% 來自合併的大中，大中已自列 A4-22。"
     "④ weight_pct 為節點合計值；weight_item 可保留組成業務線，以 \" + \" 列出"
     "（創意 A4-05＝委託設計 23.53%＋晶圓產品 75.38%＝98.91%）。"
     "若**該節點的**組成線中任一項無法可靠歸屬或計算，則該筆曝險 weight_pct=NULL，不得提供部分和"
     "（範圍是這一個節點，不是整家公司——同公司其他節點照常計算）；"
     "已知組成項仍可保留於 weight_item／evidence。"
     "★機制：db.fill_weight 對 evidence_kind='filing_too_coarse' 的列一律跳過——"
     "否則保留下來的 item 會被解析回填，等於從後門推導出節點合計。"
     "⑤ NULL weight 不代表 Indirect；Direct／Indirect 判定與營收比重是否可量化為兩個獨立問題。"
     "exposure_type='Indirect' 只給**經濟性質本身間接**者：代理/通路品項（見 R022），"
     "以及 ③ 的合併子公司情形。"
     "⑥ Revenue Exposure 等級不落地儲存，由 weight 經 view tw.v_company_industry_exposure "
     "機械推導（>=50% High／>=20% 且 <50% Medium／<20% Low，端點以 view 實作為準）；"
     "非 Revenue 型權重不產生等級。"),

    # "UPS -> C13 or I7" as drafted lets two readers file the same company in
    # different cells, which is the one thing a taxonomy cannot survive.
    ("R011", "電源類 Mapping 依應用定位判定，不留「或」",
     "Power IC→A4-07 PMIC；PSU/Server Power→C5；一般電源供應器→I7 電力電子；重電→I6。"
     "★UPS 不以名稱決定節點，依應用定位判定：資料中心/機房 UPS→C13；"
     "一般工業與商用電力電子 UPS→I7。原稿寫「C13 或 I7」，"
     "「或」讓兩個判讀者可以把同一家公司放進不同格，分類就不可重現。"),

    # Micron is not in tw.stocks, and A4/F7 are branch codes the depth check
    # rejects. The point survives with a listed company and leaf codes.
    ("R016", "跨分類依產品/供應鏈角色，不依公司名稱",
     "一家公司的曝險落在哪個分支，看的是產品與供應鏈角色，不是公司名稱或官方分類。"
     "例：群聯＝Core F7-01 記憶體模組 72.41% ＋ Minor A4-19 記憶體控制 IC 19.26%——"
     "它自研控制 IC，但主要生意是模組，兩個分支各建一列。"
     "★節點一律寫到葉節點代碼：A4／F7 是分支代碼，db.check_industry 的 [depth] 檢查會擋，"
     "要寫 A4-19／F7-01。"),

    # The draft names "A3 Memory" etc., which R005 forbids and the tree does
    # not contain. Left as drafted it is a copy-paste source of bad codes.
    ("R017", "HBM＝Theme",
     "HBM 為 Theme T07；若公司涉及 HBM，其 Industry Exposure 依實際價值鏈角色分別歸入"
     "對應節點，例如 A3-03 DRAM 製造、A5-04 HBM Packaging、A6-07 HBM Test。"
     "HBM Theme 與 Industry Exposure 可同時成立，兩者不互斥（見 R005）。"
     "★原稿寫的「A3 Memory」不是節點，樹是 leaf-node 架構，一律引用 node_code。"),

    ("R021", "時間三軸：Effective / Known / Evidence",
     "Effective_From/To＝這個分類何時成立；Known_Date＝我們最早何時可能知道（MOPS 申報日）；"
     "Evidence_Period_End＝證據本身對應哪個期間（FY2025 年報＝2025-12-31）。三者不可互相代替。"
     "回測一律以 Known_Date 過濾以避免 look-ahead；Effective_To 保持 NULL 表示仍為現行層，"
     "不可拿會計年度結束日去填，否則現行層會整批失效。"),

    ("R023", "Core 依商業本質與價值鏈角色判定",
     "營收最大業務線不得單獨決定 Industry Core；應判斷公司主要創造價值的產品、技術能力與"
     "價值鏈角色。最大營收品項不落地儲存，由 view tw.v_company_industry_exposure 的 "
     "largest_revenue_item／largest_revenue_pct 依**公司層**推導（同一公司每列相同），"
     "Core 另行判定。"
     "義隆的筆電輸入裝置模組 69.92% 大於其觸控 IC 27.72%，但它是觸控/HMI 晶片設計公司，"
     "模組是該晶片的下游載體——若逕以最大營收線決定 Core，一家 IC 設計公司會被歸成週邊模組廠，"
     "產業本質就錯了。★三條規則分工：R019＝Core 怎麼算（同節點內聚合，不得跨節點）；"
     "R023＝算完之後看什麼（商業本質與價值鏈角色，不是名稱也不是單線大小）；"
     "R024＝IC 設計與模組這個特定邊界怎麼判。"),

    ("R024", "IC 設計與模組/系統整合可並存，下游模組不得自動奪走 Core",
     "公司同時從事 IC 設計與模組/系統整合時，兩者可各自建立曝險；"
     "不得因下游模組營收較高而自動把 Core 改為模組產業。"
     "義隆＝Core A4-15 感測/HMI IC ＋ Major C11-01 筆電輸入裝置模組即為此形。"
     "★判準是兩條線的關係，不是公司屬於哪一種行業。"
     "（一）下游是自家上游能力的載體或產出＝同一能力的上下游 → Core 留上游："
     "義隆的輸入模組是它自家觸控 IC 的載體；訊芯-KY 的光收發模組 61.55% 是它拿 SiP 封裝能力"
     "做出來的產出（鴻海旗下 SiP 封裝廠），Core 留 A5-05 封裝而非 D6-01 光通訊元件。"
     "（二）兩條線均可獨立銷售且各具獨立經濟實質 → **R024 到此為止，不再提供額外判準**，"
     "回到 R019／R023，依聚合後之經濟曝險、價值創造能力與價值鏈角色判定，"
     "不以「模組」或「IC」的名稱自動決定、也不是逕取較大者。"
     "★「可獨立銷售」只證明兩條線互相獨立，不證明較大的那條就是 Core。"
     "示範什麼叫各自獨立的生意：同欣電 A5-05 封裝測試代工 69.60% 與 A1-14 陶瓷基板 17.56% "
     "分別接單；點晶 A4-18 LED 驅動 IC 93.1% 與 A4-17 馬達驅動 IC 6.9% 亦然——"
     "★這兩例的 Core 確實落在較大那條，但那是判定的**結果**不是判準，"
     "且兩例都不是「IC 設計 vs 模組」的邊界（點晶兩條線都是 IC 設計），只用來示範獨立性。"
     "★先前寫成「本業是製造/封裝的公司不適用」是錯的：訊芯與同欣電都是封裝廠，"
     "那個軸會把兩者判成同一類而得出相反於本意的結果。"),

    ("R025", "節點缺失時應新增節點，不得為維持樹狀結構而強行歸入不相符節點",
     "若公司主要業務具有明確、成熟且可獨立辨識的產業產品，而現有 taxonomy 無適當節點，"
     "不得僅為維持樹狀結構而強制歸入不相符的節點；應新增 Industry Node，並保留原有上層分類。"
     "節點代碼穩定即可安全新增：曝險以 node_code 為 FK，新增不影響既有成員。"
     "實例：碳黑（國際中橡，既非無機鹽也非橡膠製品）、不織布（恆大，非平織亦非針織）、"
     "接著劑、農用化學品、煤化工、記憶體 IC 設計、功率分立元件設計、後段測試與檢測設備、陶瓷基板"
     "——★這些**都已新增完成並在樹上**（J15 碳黑、M7 不織布、A1-14 陶瓷基板、A4-22 功率分立元件設計…），"
     "是先例不是待辦，不要再新增一次。"
     "反面警訊＝發現自己在說「最接近的是某格」而該格的定義其實排除了這個產品。"
     "★新增節點僅解決 Industry 歸屬問題，不因此自動決定 Core；Core 仍依 R019、R023 判定。"),

    ("R022", "通路/代理商的產品線不得僅據產品名稱認定製造歸屬",
     "若公司**某一業務線**僅具通路/代理性質，其產品中即使出現設備、材料、載板、IC 等製造品項，"
     "也不得僅因產品名稱將該業務認定為製造業務；該產品如需保留產業關聯，"
     "應建立為 Indirect Industry Exposure，**不得因此建立製造型 Direct 或 Core**。"
     "★但通路性質不會傳染到公司的其他業務：若年報明確揭露自有製造、產能、製造設備或製造營收，"
     "該製造業務照樣建立 Direct Exposure。例：蔚華科 V1 半導體通路 Core 93.77% Direct ＋ "
     "A2-13 測試/檢測設備 Indirect（代理的設備，比重已計在 V1）；"
     "港建 V1 Direct + 設備 Indirect。否則產業統計會把通路商錯算成製造商。"
     "判斷依據以年報為主：是否有佣金/代理性收入、產品線是否為具名製造品、正文有無自有產能描述。"
     "★官方分類僅作後驗參考不居優先（R002）——長華* 官方列電子通路業，但年報是 IC導線架 48.91% "
     "加封膠樹酯 29.29% 且無佣金收入，實為製造商；若以官方分類為首要依據會把它誤判成通路。"),
]

# 22 rules read as a list; grouped they read as a structure.
RULE_CATEGORIES = {
    "產業分類": ["R001", "R002", "R004", "R011", "R015", "R016", "R019",
             "R022", "R023", "R024", "R025"],
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
