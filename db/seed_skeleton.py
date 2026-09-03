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
     "Profit 因台灣年報普遍不揭露分部損益而無可靠來源：曝險列的 profit_exposure 欄位已移除"
     "（migration 091），不作為目前可落地的權重維度——留白待猜比沒有欄位更糟。"
     "★enum tw.weight_type 仍保留 'Profit' 這個 label（移除 enum label 會動到既有型別），"
     "但**已於 DB 層封死**：CHECK 約束 cie_no_profit_weight 讓 weight_type='Profit' 寫不進去"
     "（migration 095）。文件說禁用而程式允許，是遲早會被人踩的 policy/implementation "
     "mismatch——Core cardinality 已經是 DB 約束，這裡比照辦理。"
     "★現況限制：db.fill_weight 每次執行會清空全表的 weight_pct/weight_type/weight_basis 再回填 "
     "Revenue 型，所以目前只有 Revenue 型權重能留在庫裡；要落地 Capacity/Strategic 型，"
     "必須先讓該清空語句只作用於 weight_type='Revenue'。"),

    # R004 as drafted read as "one industry per company" to more than one
    # reader. The constraint is on the business line, not the company.
    ("R004", "同一業務曝險僅能有一個 Industry 歸屬",
     "公司可同時存在多個 Industry Exposure；不同業務線可分別歸屬不同 Industry 節點，"
     "但同一業務線不得重複計入多個節點。例：欣興的 ABF 載板歸 B3-01、硬板歸 B2-01，兩者並存；"
     "但同一條年報營收線不得同時在兩個節點計入比重（見 R020）。"),

    # "新產品" was too wide: a product can be new and still have a home.
    ("R005", "尚未有適當 Industry 節點的新興技術/產品優先進入 Theme",
     "若技術／產品尚無合適 Industry Node，**於新增節點前**可暫以 Theme 表達其市場概念；"
     "一旦建立對應節點，仍依 R025 建立 Industry Exposure。"
     "★Theme 是**暫時性／跨產業的表達**，Industry 才是最終歸屬——"
     "本條不是 R025 的替代路徑：「找不到節點就丟 Theme」是誤讀，R025 要的是去新增節點。"
     "若已有適當節點，Industry 與 Theme 可並存，不必二選一。"
     "HBM 即為此例：Theme T07 與 A3-03 DRAM 製造／A5-04 HBM Packaging／A6-07 HBM Test "
     "同時成立（見 R017）。★現況（2026-09-02）：T07 有 13 家成員，但 A5-04／A6-07 皆 0 筆曝險，"
     "**並存形態在庫裡尚無實例**——這條規則說的是允許並存，不是已經並存。★節點一律引用 node_code，不引用「A3 Memory」這類非節點的口語名——"
     "A3 底下沒有 Memory 格，只有 A3-03 DRAM 製造與 A3-07 NOR/NAND Flash 製造。"),

    ("R015", "A3 兩維度：製造模式 × 產品類型",
     "Foundry／IDM＝製造模式；Memory／Power／CIS／Compound＝產品類型。"
     "★兩者可同時存在、不互斥：一家 IDM 可同時掛 Power 與 Compound 產品節點；"
     "Foundry 不是產品分類，不可被讀成 A3 的一個產品格。"
     "★同一公司可有多個**產品類型**曝險，但**製造模式本身不得作為 Industry Exposure**："
     "旺宏的 Manufacturing Model 是 IDM、Industry Exposure 是 Memory（A3-03／A3-07），"
     "不得寫成「Industry Exposure = IDM」。製造模式描述公司怎麼生產，不是它生產什麼。"),

    # Theme leaves the exposure type: it has its own table and its own contract.
    ("R018", "Exposure Type 只有 Direct / Indirect；Confidence 另計",
     "Type：Direct＝該公司**自身直接從事、創造或提供**之該產業業務——"
     "不限於自有製造，IC 設計、測試服務、封裝服務、設計服務皆為 Direct；"
     "Indirect＝該產業曝險在經濟歸屬上不應視為該節點之直接業務，"
     "包括代理／通路性質（見 R022），以及市場層級已由同一 Industry node 之合併子公司"
     "獨立計入而會造成重複計數者（見 R020③）。"
     "★不可簡化成「該營收已被他處計入」——代理商的營收是它自己的帳，並沒有被別人計過，"
     "間接的是它與該製造節點的關係。"
     "Theme 不是 exposure type——題材有獨立表與獨立契約（無營收判準、evidence 必填），"
     "若容許 exposure_type='Theme' 會與 R003「Theme 不可作為 Primary Industry」自相矛盾。"
     "★evidence_kind 記錄**證據類型**，confidence 記錄**對該 Exposure 判斷的可信程度**，"
     "兩者不得視為同義欄位、也不得由前者機械映射後者："
     "欣興「印刷電路板 99.07%」是品質極高的財報證據，卻無法支持任何單一節點的配置——"
     "證據品質高、節點歸屬信心不可得，兩件事同時成立。"
     "★定義：confidence＝對**該 Industry Exposure 歸屬判斷本身**的信心，"
     "不是對來源文件可信度的評價（後者是 evidence_kind 的事）。"
     "★刻度（量的是歸屬判斷，不是文件品質）："
     "A＝年報品項直接指名該節點的產品，換一個判讀者會得到同一格（台橡「合成橡膠 98.18%」→J3）；"
     "B＝需要一層產業知識才對得上，但對上之後沒有第二種合理歸屬"
     "（義隆「筆電輸入裝置模組」→C11-01）；"
     "C＝證據支持「有這門生意」但不足以定到這一格，或存在第二種合理歸屬"
     "（欣興「印刷電路板 99.07%」拆到 B3-01／B2-01 屬此級）；"
     "D＝待確認，尚未有足以支撐的證據。"
     "★同一份年報可以同時是 A 級證據與 C 級信心——欣興就是（見上）。"
     "★選填欄位：沒把握就留 NULL，NULL 表示「尚未評估」而非 D。"
     "★現況（2026-09-02）：confidence **0/523 已填**，上面三個範例是刻度的示範而非庫裡的實例；"
     "這段刻度目前是待建欄位的規格。"
     "★曝險層**刻意沒有 source 欄位**，四個欄位已經把該分的分開了："
     "evidence＝支持這筆判定的原文／依據、evidence_kind＝該依據的類型、"
     "known_date＝最早何時可被知道、confidence＝對這筆歸屬判斷的信心。"
     "tw.company_theme_mapping 與 tw.company_ecosystem_mapping **有** source，"
     "是因為它們問的問題不同（「為什麼把公司掛到這個題材」需要指名出處，"
     "生態系還多一個 relationship_type：客戶／供應商／平台）。"
     "★**三張對照表欄位不一致是設計不是缺陷**，MUST NOT 為了正規化而把 source 補進曝險層，"
     "或反過來把曝險層的欄位硬套到那兩張表。題材／生態系的證據治理是獨立議題，另案處理。"),

    ("R019", "Core Industry 依曝險資料裁定並記錄，不以單一營收最大值機械決定",
     "三段式：**Exposure Table → 同一 node 聚合後形成 Core 候選集合 → 依 R023／R024 裁定 → Core**。"
     "★**候選集合的定義（可直接照著實作）**：在該 as-of 有效的 Industry Exposure 中，"
     "`exposure_type='Direct'` 的**葉節點**（R016），且該曝險的**業務狀態不是 Emerging**，"
     "先依相同葉節點聚合可可靠權重，即為 Core Candidate Set。"
     "★**候選資格 MUST NOT 依賴既有的 role='Core'／'Major'／'Minor' 判定結果**——"
     "Core 是本規則的**產物**，拿它當輸入就形成自我增強的迴路："
     "上次是 Core → 這次仍有資格 → 這次又是 Core。同理，「不逐檔手填 core_industry」"
     "若改成從 role 餵回來，只是把手動維護換了個入口。"
     "★唯一可用的資格條件是**業務狀態**：依 R026 的定義，尚無可辨識營收且僅有布局 evidence "
     "者為 Emerging，不進候選；其餘皆進。**此判定應回到 R026 的兩個條件，"
     "不得只讀 role 欄位的字面值**。"
     "★★已知 schema 缺陷：`role` 這一欄同時裝了兩種東西——Core／Major／Minor 是**推導結果**，"
     "Emerging 是**業務狀態事實**。實作上今天只能寫 `role IS DISTINCT FROM 'Emerging'`"
     "（用 IS DISTINCT FROM 而非 `<>`，NULL 才不會靜默濾掉整列）；"
     "乾淨的作法是另立 business_status 欄把狀態與排名分開。"
     "目前 Emerging 0 列所以還沒有實害，但這是本規則唯一沒有被 schema 支撐的地方。"
     "★**同一公司、同一 Leaf Node 只能形成一個候選項——候選集合以 `(stock_id, node_code)` 去重**。"
     "公司有 A5-02 30%、A5-02 20%、A6-01 40% 三列時，候選是 A5-02=50% 與 A6-01=40% 兩項，"
     "不是三項；實作若 `GROUP BY stock_id, node_code, id` 會把同一節點當成多個候選互相競爭。"
     "（該節點的合計怎麼算、何時整筆變 NULL，見 R020④。）"
     "★**「在該 as-of 有效」必須寫成謂詞**：`effective_from <= as_of AND "
     "(effective_to IS NULL OR as_of < effective_to)`（區間語意見 R029）。"
     "as-of＝今天時它退化成 `effective_to IS NULL`，那只是**特例不是定義**——"
     "回測在過去某日重算 Core 時，用 `effective_to IS NULL` 會拿到今天的分類而不是當時的。"
     "★不可省略這個謂詞：歷史層的 Exposure 一旦被拿進候選，"
     "同一家公司會同時出現已失效與現行的節點在競爭 Core。"
     "★`exposure_type='Indirect'` **不得參與 Core 候選**——它依 R018 就不是該公司自身的直接業務，"
     "若放進來競爭，蔚華科那種「V1 通路 Direct 93.77% ＋ A2-13 設備 Indirect」的公司"
     "就可能被推出一個製造型 Core，與 R022 正面衝突。"
     "★**業務狀態為 Emerging 者不得成為候選**——依 R026 尚無可辨識營收，不是現有核心業務；"
     "日後有實際經濟活動須重建 Direct Exposure 再依 R019～R024 裁定。"
     "★**候選資格不以 `weight_pct IS NOT NULL` 為條件**：有可靠權重者以權重作支持證據，"
     "weight 為 NULL 者照樣進入候選並依 R023／R024 裁定——超豐無比重仍為 Core 就是此形。"
     "實作若寫成 `WHERE weight_pct IS NOT NULL` 會把整批只揭露粗線的公司排除在 Core 之外。"
     "候選以產業曝險資料為基礎產生，最終歸屬再依 R023／R024 所定義之主要經濟曝險、價值創造能力、"
     "價值鏈角色與公司產業定位判定。"
     "★**MUST NOT 以 `MAX(weight_pct)` 取代規則判斷**——義隆 C11-01 69.92% 大於 A4-15 27.72%，"
     "純 SQL 取最大值一定得到 C11-01，而規則明定 Core 是 A4-15。"
     "**「不手動維護」指的是不另立一個手填的 core_industry 欄位**，"
     "不是說這一步可以自動算出來——R023 的 ①～⑤ 本來就需要人對證據做判斷。"
     "★**Core 的實況是「裁定後記錄」不是「計算」**：role 目前由 db/industry_data.py 逐列鍵入，"
     "載入器與 db.check_industry **驗證**它符合規則，而不是產生它。"
     "把這件事寫清楚，是為了不讓標題變成一個沒有實作在後面的宣稱（R006 的 Profit 同型）。"
     "★**候選只有一個時 Core 是恆等式**（現況 383 家），不需要理由；"
     "**候選有兩個以上時 R023 才真的被行使過**（現況 64 家），該次裁定的理由 MUST 寫進 "
     "Core 那一列的 evidence，以「Core 理由：」起頭——沒有記錄理由的多候選裁定，"
     "由 db.check_industry 的 [core_reason] 列出未記錄者。"
     "★候選集合本身已實作為 `db/core_candidates.py`（`python -m db.core_candidates`），"
     "四個謂詞逐條對應本規則、支援 `--as-of` 重算歷史時點；[core_reason] 直接呼叫它，"
     "候選集合只有一份實作。它同時檢出 Core 不在自己候選集內的情形"
     "（Indirect、掛在分支節點、或候選集為空），那是其他檢查看不到的。"
     "★**多個業務群之間如何取捨見 R023**——本條的兩個例子"
     "（精材 50.16 > 47.86、中石化 J9 50.157 > J1-01 49.445）最後都落在較大的那一群，"
     "那是判定的結果不是判準；義隆的 C11-01 69.92% 就沒有拿到 Core。"
     "★只有確實屬於同一 Industry node 的業務線才能聚合，**不得因上層產業、供應鏈或終端應用相近"
     "而跨節點合併**，亦不得跨節點為取得較大比重而合併："
     "A5 封裝 45% 與 A6 測試 48% 不可併成「半導體 93%」再據以定 Core，那等於把 taxonomy 拆掉；"
     "中石化的己內醯胺/尼龍粒 50.16%（J9）與丙烯腈 49.45%（J1-01）分屬兩節點，"
     "不得合併成「石化 99%」；本例依公司主要產業定位判定 Core 為 J9，"
     "而非因兩者合計後的 99% 得出。"
     "例：精材測試服務 47.86% 大於晶圓級尺寸封裝 44.98%，但後者與後護層封裝 5.18% 同屬封裝業務群"
     "合計 50.16%，故 Core 為 A5-02 而非 A6-01。"
     "★若無法唯一判定（如 42%／38%／20% 三群資料完整但商業本質分不出主從），"
     "該公司視為 **Core pending**：資料庫**不建立 Core row**，由 db.check_industry 的 [core] "
     "列為 validation finding；載入器不因此拒收。"
     "★Core pending 是資料的狀態不是欄位——schema 沒有 core_status，也不需要加。"
     "★不得改以 role=NULL 表示（role 是列上的 enum，沒有 NULL 這個值），"
     "更不得因為要湊出一個 Core 而隨便挑一群。**沒有 Core 不等於沒有主要產業**，"
     "只是這家公司的主從還沒判定。"
     "★此例的節點合計必須真的落地（R020④）：A5-02 的 weight_item 是"
     "「晶圓級尺寸封裝 + 晶圓級後護層封裝」、weight_pct=50.16——若只存 44.98，"
     "資料本身會顯示 Core 小於 Major，規則的論證在庫裡就是假的。"
     "★Core ≠ Revenue Exposure Level：超豐無比重仍為 Core。前者是產業定位，後者是營收分箱。"
     "★現況（2026-09-02）：447 家各有恰好 1 個 Core，**Core pending 至今 0 例**；"
     "且 381/447（85%）只有一筆曝險，三段式在那些公司上是恆等式，"
     "真正有裁定作用的是其餘 66 家多曝險公司。"),

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
     "③ **判準只有一個：母公司這一筆曝險，會不會與子公司的獨立資料構成同一經濟活動的"
     "重複統計**——會，則母公司該列為 Indirect。子公司佔多少、是否已上市，都不是判準。"
     "適用情形是母公司合併營收線包含已於同一 Industry node 獨立建檔之合併子公司，"
     "且該營收已由該子公司在市場層級獨立計入。"
     "★不可寫成「主要來自」——那會把門檻變成爭論點；也不可因為「子公司已上市」就整筆判 "
     "Indirect：若該營收線同時含母公司自身的其他業務，重複的只是其中一部分，"
     "此時整列 weight 本來就是 NULL（②），Indirect 與否仍回到上面那個判準。"
     "該 Indirect 列的 weight_pct 一律 NULL，比重僅保留於 evidence——年報確實揭露了這個數字，"
     "不寫明就會有人依 ① 把它填進去，重複計數就從這裡進來。"
     "例：茂達離散式功率元件 44.91% 來自合併的大中，大中已自列 A4-22。"
     "④ weight_pct 為節點合計值，定義為「**可可靠拆分且屬於該 node 的 leaf revenue lines 總和**」"
     "——只有 leaf 組成的 node total 才是正式 weight：若年報同時列出 A5 50%、A5-01 20%、"
     "A5-02 30%，正式 weight 是 A5-01 的 20% 與 A5-02 的 30%，A5 那個 50% 是 subtotal，"
     "不另建一列（與 R027 分母同一個原則）。weight_item 可保留組成業務線，以 \" + \" 列出"
     "（創意 A4-05＝委託設計 23.53%＋晶圓產品 75.38%＝98.91%）。"
     "若**該節點的**組成線中任一項無法可靠歸屬或計算，則該筆曝險 weight_pct=NULL，不得提供部分和"
     "（範圍是這一個節點，不是整家公司——同公司其他節點照常計算）；"
     "已知組成項仍可保留於 weight_item／evidence。"
     "★機制：db.fill_weight 對 evidence_kind='filing_too_coarse' 的列一律跳過——"
     "否則保留下來的 item 會被解析回填，等於從後門推導出節點合計。"
     "⑤ NULL weight 不代表 Indirect；Direct／Indirect 判定與營收比重是否可量化為兩個獨立問題。"
     "**Indirect 的完整定義只寫在 R018 一處**，本條與 ③ 不重複列舉——新增第三種間接情形時"
     "只改 R018，避免三處各改一半而自相矛盾。"
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
     "例：群聯＝Core F7-01 記憶體模組 72.41% ＋ Major A4-19 記憶體控制 IC 19.26%——"
     "它自研控制 IC，但主要生意是模組，兩個分支各建一列。"
     "★**Exposure 的 node_code 必須指向葉節點**（在 tw.industry_tree 中沒有任何 "
     "parent_code 指向它的節點）；非葉節點只供聚合與查詢，**不得直接建立 Exposure**。"
     "A4／F7 是分支代碼，要寫 A4-19／F7-01；db.check_industry 的 [depth] 檢查在守這條。"
     "★葉的定義是**結構性的不是層級性的**：樹目前是三層，但若日後出現 A4-15-01，"
     "A4-15 就不再是葉，既有指向它的曝險必須一併下移——「L3 就是葉」不成立。"
     "★**節點在仍有現行 Exposure 指向時，不得直接被降為非葉**：新增 child 之前必須先完成"
     "既有 Exposure 的**重新歸屬**，使所有現行 Exposure 均重新指向葉節點。"
     "★是「重新歸屬」不是「下移」：A4-15 拆成 A4-15-01／A4-15-02 時，原有業務可能要拆到兩個新葉、"
     "只移到其中一個、或因重新判定而建立不同的 mapping，「下移」只描述了其中一種。否則 FK 照樣解得開、什麼都不會報錯，那些列卻已經違反本條，"
     "要等 [depth] 事後才抓到。★DB 層已強制：`tw.industry_tree` 的 BEFORE INSERT trigger "
     "`industry_tree_leaf_guard` 會擋下「父節點尚有現行曝險」的新增（migration 096），"
     "錯誤訊息直接指名還有幾列要搬。"),

    # The draft names "A3 Memory" etc., which R005 forbids and the tree does
    # not contain. Left as drafted it is a copy-paste source of bad codes.
    ("R017", "HBM＝Theme",
     "HBM 為 Theme T07；若公司涉及 HBM，其 Industry Exposure 依實際價值鏈角色分別歸入"
     "對應節點，例如 A3-03 DRAM 製造、A5-04 HBM Packaging、A6-07 HBM Test。"
     "HBM Theme 與 Industry Exposure 可同時成立，兩者不互斥（見 R005）。"
     "★原稿寫的「A3 Memory」不是節點，樹是 leaf-node 架構，一律引用 node_code。"),

    ("R021", "時間三軸：Effective / Known / Evidence",
     "Effective_From/To＝這個分類何時成立；"
     "Known_Date＝**本系統認定可取得該分類證據的最早日期**，"
     "目前以 MOPS 申報日作為統一的 operational definition"
     "（公司新聞稿、法說、重大訊息可能更早，未來若納入不會推翻本定義，只是換一個更早的取值）；"
     "Evidence_Period_End＝證據本身對應哪個期間（FY2025 年報＝2025-12-31）。三者不可互相代替。"
     "回測一律以 Known_Date 過濾以避免 look-ahead；Effective_To 保持 NULL 表示仍為現行層，"
     "不可拿會計年度結束日去填，否則現行層會整批失效。"
     "★known_date 為 NULL 的現行列，對任何依 known_date 過濾的回測**等同不存在**，"
     "屬缺陷不是可接受狀態；db.check_industry 的 [known] 非零時必須有結案理由"
     "（通常是該公司尚無 tw.product_revenue_status.filing_date，補到就會自動回填）。"
     "★**evidence_kind 非年報者（company_site／investor_conf），known_date 應取該證據自身的"
     "公開日，不得沿用年報申報日**——證據不是年報，年報申報日既不是最早也不是正確。"
     "現況（2026-09-02）：34 列屬此情形且都掛著年報申報日，是已知缺陷；"
     "在補到各自的公開日之前，這些列的 known_date 只能當作保守上界看。"
     "★★**更正 ≠ 變動，兩者不得共用同一條時間軸**：改寫我們對某期間的判讀是**更正**，"
     "應**就地取代**該列；只有事實本身改變（新年度年報到、公司真的轉型）才開新的歷史層。"
     "違反的後果是寫下從未為真的歷史：精材曾因漏算第二條線而存 44.98%，"
     "若以換代處理，資料庫就會宣稱「某段期間精材的封裝比重是 44.98%」，"
     "而 as-of 落在那段的回測會忠實重播一個已知錯誤——那不是防 look-ahead，是保存錯誤。"
     "★判準是**證據期間**不是編輯日期：`db.seed_industry` 的 `--effective-from` "
     "預設為 EVIDENCE_PERIOD_END（FY2025＝2025-12-31），同期間內的差異一律就地更正、"
     "同期間內被移除的成員直接刪除；要開新層是傳入新的期間（FY2026＝2026-12-31）。"
     "★先前以「編輯當天」當判準，等於同一個錯誤早一天發現就消失、晚一天發現就變成假歷史；"
     "2026-09-03 已依此清掉 18 列這類更正歷史（備份於 tmp/backup/），"
     "並把它們的 effective_from 歸回證據期間。"),

    ("R023", "Core 依商業本質與價值鏈角色判定",
     "營收最大業務線不得單獨決定 Industry Core；應判斷公司主要創造價值的產品、技術能力與"
     "價值鏈角色。最大營收品項不落地儲存，由 view tw.v_company_industry_exposure 的 "
     "largest_revenue_item／largest_revenue_pct 依**公司層**推導（同一公司每列相同），"
     "Core 另行判定。"
     "義隆的筆電輸入裝置模組 69.92% 大於其觸控 IC 27.72%，但它是觸控/HMI 晶片設計公司，"
     "模組是該晶片的下游載體——若逕以最大營收線決定 Core，一家 IC 設計公司會被歸成週邊模組廠，"
     "產業本質就錯了。★三條規則分工：R019＝Core 怎麼算（同節點內聚合，不得跨節點）；"
     "R023＝算完之後看什麼（商業本質與價值鏈角色，不是名稱也不是單線大小）；"
     "R024＝IC 設計與模組這個特定邊界怎麼判。"
     "★**裁定優先序（依序判定，前一項能定案就不往下走）**。"
     "**「能定案」＝證據足以排除其他候選節點之合理競爭解釋**——"
     "若另一個候選仍有說得通的解釋沒被排除，這一階就不算定案，必須繼續往下走。"
     "（否則 A 說「技術能力在 A4」就宣稱定案，B 說「A5 也合理」，優先序等於沒有作用。）"
     "只寫判斷原則不寫順序，兩個研究員可以各自宣稱符合 R023 而得到不同的 Core："
     "① **主要且持續創造經濟價值的核心技術／產品能力**落在哪個節點——"
     "「持續創造經濟價值」是限定語，**不是「技術含量高就勝出」**：一項技術再深，"
     "若已不再是公司經濟價值的主要來源，就不在這一階勝出；"
     "② **價值創造位置**：主要毛利、技術、IP、品牌價值由哪個節點產生；"
     "③ **商業本質**：下游產品是否為上游核心產品的載體、組裝或延伸（是 → Core 留上游，見 R024）；"
     "④ **長期策略定位**：公司對外定位、研發資源、資本支出主要指向哪個節點；"
     "⑤ **營收比重**：**只作支持證據，不得單獨作為決策依據**。"
     "義隆走完就是：核心能力＝觸控/HMI 晶片設計（①）→ 輸入模組是該晶片的下游載體（③）"
     "→ Core = A4-15；69.92% 的模組營收落在 ⑤，只是支持證據。"
     "★**反否決條件——Core 最終採納前的必要檢查，不是補充說明**："
     "若候選依 ①～⑤ 取得優先地位，但其業務僅屬歷史遺留、已逐步退出、"
     "或公司已明確將資源移轉至另一業務，**該候選不得取得 Core**；"
     "應**回到剩餘候選重新依 ①～⑤ 裁定**，若無其他候選可唯一判定，則為 **Core pending**（R019）。"
     "**不得僅因既有技術能力保留而取得 Core。**"
     "舊產品 40%／新產品 45% 而公司已全面轉型時，Core 不應永遠卡在技術壁壘較高的舊產品——"
     "這條對時間序列尤其重要：Core 必須會隨轉型移動，否則歷史層記的是一個過期的公司。"
     "★★**taxonomy 動作使候選集合改變時，MUST 重跑 Core 裁定**——觸發有二：(a) **新增 Industry Node**；(b) **既有 Node 的 Definition 擴充**（後者一樣會讓原本無處可掛的業務變成合法候選，2026-09-03 J4-01 擴充納入界面活性劑後，和桐的脂肪醇系產品即由「無節點」變成 Core 候選）。兩者皆：當新增 Industry Node 讓原 Core 出現"
     "新的高權重候選，不得因原 Core 已存在而直接沿用；流程是「新增節點 → 曝險重算 → 候選集合改變"
     "→ 依 ①～⑤ 重裁 → Core」，而不是「新增節點 → 保留原 Core」。"
     "否則分類系統會產生路徑依賴：新增的節點永遠無法挑戰既有 Core，先建檔的那一格就永久勝出。"
     "實例：3663 鑫科新增 L10 貴金屬（55.97%）後，候選從「A1-09 靶材 8.53% vs L9/L6」變成"
     "「A1-09 8.53% vs L10 55.97%」，必須重裁——重裁結果仍為 A1-09，但那是走完 ①～⑤ 的結論，"
     "不是沿用（理由記在該列 evidence）。"),

    ("R024", "IC 設計與模組/系統整合可並存，下游模組不得自動奪走 Core",
     "公司同時從事 IC 設計與模組/系統整合時，兩者可各自建立曝險；"
     "不得因下游模組營收較高而自動把 Core 改為模組產業。"
     "義隆＝Core A4-15 感測/HMI IC ＋ Major C11-01 筆電輸入裝置模組即為此形。"
     "★判準是兩條線的關係，不是公司屬於哪一種行業。"
     "（一）下游是**公司自身具實質技術／IP／製造能力**的上游業務之載體或產出"
     "＝同一能力的上下游 → Core 留上游。★「自身具實質能力」是必要條件："
     "若上游產品是外購、公司真正創造的價值在下游系統整合，本款不適用，回到 R023 的優先序。例："
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
     "★新增節點僅解決 Industry 歸屬問題，不因此自動決定 Core；Core 仍依 R019、R023 判定。"
     "★**只有現有 Node 的產品定義不足以容納該業務時才新增 Node**；"
     "若僅為名稱或描述不精確，應優先**修正既有 Node 的名稱與定義**，不得無必要增加節點——"
     "否則分類樹會過度碎片化，每家公司一格等於沒有分類。"
     "例：A4-15 原名不夠精確，改名為「感測／HMI IC」即可容納義隆的觸控晶片，"
     "不需要另開 A4-15-01；而碳黑無論怎麼改名都塞不進無機鹽或橡膠製品，那才是該新增的情形。"
     "★判斷句：問「是這一格說不清楚，還是這一格根本不包含這個產品」——前者改名，後者新增。"
     "★★**新增節點前 MUST 先逐一檢查所有既有候選父節點的產品定義**（不只是最像的那一格）；"
     "若擴充某個既有節點的名稱／定義即可**無歧義**涵蓋該產品，應修正該既有節點，"
     "**不得新增子節點**。沒做過這個檢查就新增的節點，視為未依本規則產生。"
     "理由：碎片化不是一次發生的，是每次「這格好像不太合，開一格比較快」累積出來的；"
     "有了前置檢查，1560 中砂的研磨切削工具、2483 百容的步進馬達、6789 采鈺的微型光學元件"
     "才會一致地走「擴充既有節點」，而不是各開一格。"
     "★★★但「無歧義涵蓋」是硬條件不是傾向。反面教材：5274 信驊的伺服器管理控制器（BMC）"
     "**不得**併入 A4-08 微控制器 IC (MCU)——兩者技術上都是控制器，但 BMC 是伺服器主機板的"
     "頻外管理與平台信任根，客戶、通路、景氣循環與競爭者都與泛用 MCU 不同。"
     "**技術上同屬一類 ≠ 產業分類上同一節點**；擴充定義若會讓兩門不同的生意落進同一個比較池，"
     "那就不是無歧義涵蓋，該新增節點。"),

    # role had no threshold at all: Major ran down to 6.90% while Minor ran up
    # to 19.26%, so the same share was labelled both ways.
    ("R026", "Major / Minor / Emerging 的判準：數值為預設，偏離須具名",
     "Core 依 R019／R023 判定（產業定位，不由數字決定）；**非 Core 的 role 走數值預設**："
     "weight_pct >= 15% 為 Major、< 15% 為 Minor。"
     "★weight_pct 為 NULL 且非 Core 者，**依序判定**：先問「有沒有 evidence 證明它是附屬業務」"
     "——有 → Minor；沒有 → Major。**Major 是 default 不是判定結果**，"
     "寫成「預設 Major，再看看要不要改 Minor」會讓人以為 Major 是已經判過的結論。"
     "★「附屬業務」須有 evidence 支持，**不得僅因 weight 為 NULL 或營收未揭露而認定**，"
     "否則這個詞會變成下一個不可稽核的判斷。"
     "★這個預設不是「不知道就算重要」：能被建成一列代表該業務已被確認存在（evidence 必填），"
     "預設 Major 的意思是「無法量化、已確認存在、且沒有足夠理由認定它是附屬」。"
     "Emerging 須**同時**滿足兩件事：(一) 該業務**尚無可辨識營收**（weight 必為 NULL）；"
     "(二) 已有**明確布局 evidence**（擴產、合資、取得認證、法說會列為新事業等）。"
     "★只要已有可辨識營收，即應依 Direct/Indirect 與 weight 規則判定，**不得標 Emerging**——"
     "營收 1% 但大規模投入的公司是 Minor 不是 Emerging；Emerging 不是「比重很小」的同義詞，"
     "也不是「很看好」的同義詞。"
     "★數值預設**不適用**於兩種列，否則會互相吃掉：(一) role='Emerging'——它的 weight 本來"
     "★★Emerging 與 Core／Major／Minor **不是同一種東西**：前者是**業務狀態**"
     "（尚無可辨識營收），後者是**推導出來的重要性排名**。兩者共用 role 欄位是既有 schema "
     "的權宜，不是設計主張——R019 的候選資格只能引用前者，引用後者會讓 Core 自我增強。"
     "就必為 NULL，套用「NULL 預設 Major」會讓 Emerging 永遠不存在；"
     "(二) exposure_type='Indirect'——依 R020③ 其 weight 一律 NULL，套用預設會把「代理的線」"
     "或「子公司已計過的線」標成公司的重要業務。★這兩種列既然沒有 weight，role 依 "
     "**evidence 所支持之實際經濟重要性或策略重要性**判定，並於該列 evidence 記載判定理由——"
     "沒有 weight 不代表沒有依據，但依據必須寫出來。"
     "例：蔚華科 A2-13、茂達 A4-22、港建* B5-01 三列 Indirect 皆為 Major，"
     "因為 evidence 顯示該曝險對公司具有實質經濟重要性，"
     "但依 R018／R020③ 不作為該 node 的 Direct weight——"
     "**對公司重要**與**對該節點是直接曝險**是兩個問題。"
     "★偏離預設時，理由必須寫進該列 evidence——沒有理由的偏離就是不可重現。"
     "★為什麼要有這條：訂定前 Major 最小 6.90%（點晶馬達驅動 IC）而 Minor 最大 19.26%"
     "（群聯控制 IC），同一個區間兩種標法都出現過，這一欄等於無法稽核。"
     "★15% 而不是 20%：20% 會與 R020⑥ 的 Medium 分箱同點，role 就退化成 revenue_exposure "
     "的複本；role 要回答的是「這門生意對公司重不重要」，不是「營收落在哪一箱」。"
     "★db.check_industry 的 [role] 檢查列出偏離預設的列（Major<15 或 Minor>=15）；"
     "它不判對錯——有具名理由的偏離是合法的——但沒有它，這一欄會無聲漂回訂定前的狀態。"
     "★現況（2026-09-02）：Core 447／Major 72（Direct 69＋Indirect 3）／Minor 4／**Emerging 0**；"
     "4 筆 Minor 的 weight 全部有值（6.64–7.87%），即「weight NULL 且非 Core 標 Minor」**0 例**。"
     "Emerging 的排除條款與「附屬業務須有 evidence」目前都還沒有被行使過。"),

    # A node that was never built looks exactly like a business the company
    # does not have. This is the largest silent gap in the table.
    ("R027", "未被曝險覆蓋的營收＝尚未建檔，不等於公司沒有該業務",
     "曝險表不保證窮盡一家公司的營收。**未覆蓋的營收只代表尚未建檔，不得被讀成"
     "「該公司沒有其他產業曝險」**；任何以曝險表做產業統計、同業比較或曝險加總的下游，"
     "必須先看該公司的覆蓋率，否則會系統性低估多角化公司。"
     "★覆蓋率＝**Direct 且 weight_type='Revenue' 的 weight_pct 合計** ÷ "
     "年報可辨識之**互斥、最底層**營收線合計。"
     "分子鎖定 Revenue 型：R006 保留了 Capacity／Strategic 型權重，那些不是營收占比，"
     "混進來覆蓋率就沒有意義。"
     "★分母的定義是：**僅計互斥、最底層、可識別產品／業務線**；subtotal、合計、total "
     "及不指名產品之列均排除。「電子產品 70%（下含 IC 40%、模組 30%）＋化學品 30%」若四列全計，"
     "分母會變成 170%，覆蓋率直接壞掉——排除 subtotal 的理由是它**不是 leaf**，"
     "不是它「不是產品」。"
     "★實作上以列名清單當 parser heuristic（其他、其它、合計、其他營業收入、租賃收入、勞務收入、"
     "銷貨收入、銷貨淨額、營業收入、營業收入淨額、銷貨收入淨額、總計，以及「減:」開頭的沖銷列）；"
     "清單服務於上面那個定義，不是定義本身，遇到清單沒收錄的 subtotal 仍應排除。"
     "（「銷貨收入」出現在 51 家、平均 40.6%，排除的理由是**列名不指名產品**，"
     "不是該筆營收不算產品。）"
     "★已知限制：年報解析偶爾會把正文片段當成營收列（「◆ 搭配 21%」「單位:新台幣(仟元) 6%」），"
     "目前仍有 14 家公司分母 >105%（最高 134.54%）；這些公司的 coverage 屬**解析異常下的非可靠值**，"
     "不應視為正常量測結果。★不可寫成「下限」——分母被撐大會低估，但漏抓營收線會高估，"
     "沒有證據顯示誤差只有單一方向。"
     "★db.check_industry 的 [coverage] 檢查列出覆蓋率 <70% 的公司（門檻可用 "
     "--coverage-threshold 調）；此為**提示不是阻擋**，載入器不因覆蓋率低而拒收——"
     "補不補是排程問題，但看不見才是資料問題。"
     "★**有比重表卻一格都沒指派**的公司也必須出現在這張表上（覆蓋率 0%），"
     "所以該檢查以「已分類公司」為左表 LEFT JOIN 權重合計，不可用 INNER JOIN——"
     "內接會把最糟的那 82 家整批濾掉，正好漏掉本條要防的東西。"
     "這 82 家分兩種，規則對它們的處置不同：年報只給一條粗線的（臻鼎-KY「各式印刷電路板 "
     "100%」）屬 R020② 的 filing_too_coarse，是**已結案**；年報有可拆的比重表卻尚未去讀的"
     "屬**待補**，不得因為 weight 是 NULL 就當成「未揭露」。"
     "★**低覆蓋率有四種成因，不可混為一談**——[coverage] 的 coverage_status 欄回答"
     "「這個 58% 是什麼意思」：`parser_anomaly`（分母本身就錯，見上）／"
     "`coarse_filing`（該公司每一列都是 filing_too_coarse，沒有東西可指派，**已結案不是待補**）／"
     "`incomplete_mapping`（年報拆得比曝險細，**真的待補**）／`reliable`（達到門檻）。"
     "★四個值是同一套字彙，`reliable` 也是狀態不是「不在表上」——"
     "門檻決定誰被列出，不決定誰被標成什麼；要看全表含狀態就把門檻拉過 100"
     "（`--coverage-threshold 101`，得 408 家）。"
     "下游若要拿覆蓋率做自動選股或統計，MUST 先看 status——把 coarse_filing 當成資料缺漏"
     "會把一整批只能揭露一條粗線的公司誤判成建檔不全。"
     "現況分布（2026-09-02）：incomplete_mapping 155／coarse_filing 8／parser_anomaly 1。"
     "★家數與名單**以 db.check_industry [coverage] 的當次輸出為準**，不寫進規則正文——"
     "曝險一改數字就變，寫死的統計會讓規則長期處於「已知錯誤」狀態"
     "（本條初稿寫 165 家，隔一次修正就變成 164，正是這個問題）。"),

    # The judgement columns exist and are empty; without a filling rule they
    # will either stay empty forever or get filled by whatever seems handy.
    ("R028", "判斷型欄位為選填，填了就要能交代；空值代表尚未評估",
     "confidence（刻度見 R018）、strategic_priority、strategic_evidence、supply_chain_position "
     "四欄皆為**選填**，NULL 一律解讀為「尚未評估」，不得解讀為最低級或「無」。"
     "★strategic_priority（High/Medium/Low/Unknown）量的是**公司自己**對該業務的資源投入"
     "與對外承諾，不是我們認為它重要——填 High/Medium/Low 時必須同時填 strategic_evidence"
     "（法說會、年報營運計畫、擴產公告等具名出處）；沒有出處就填 Unknown 或留 NULL。"
     "★supply_chain_position 取自 tw.supply_chain_position 字典，描述該**曝險**在鏈上的位置"
     "而非公司（同一家公司不同節點可以不同，見 R007）。"
     "★這四欄不在鏡像檔 db/industry_data.py 裡，是直接寫在 DB 的判斷層；"
     "db.seed_industry 換代時以 _carried() 四欄一併原樣帶過（migration 091 記載的漏帶陷阱已修，"
     "並以換代實跑驗證）。四欄皆已由 view tw.v_company_industry_exposure 暴露"
     "（supply_chain_position 於 migration 093 補上），下游看得到才有人會去填。"
     "**不得由 evidence_kind、weight_pct 或 role 機械推導填入任一欄**——那會製造出"
     "看起來有判斷、實際只是既有欄位換句話說的資料。"
     "★現況（2026-09-02）：四欄**填充率皆為 0/523**。換代帶過已實跑驗證，"
     "但驗到的是搬運 NULL——這四欄的資料品質尚未開始建立。"
     "★**首批完成度門檻**（不是每欄都要 100%，是給這份 Master DB 一個「可以開始被引用」"
     "的定義）：Core 100%（已達）／Industry Exposure 100%（已達，447 家皆有列）／"
     "known_date >90%（現 521/523＝99.6%，但其中 34 列的日期取錯來源，見 R021）／"
     "confidence >90%／supply_chain_position >50%／strategic_priority >30%／"
     "ecosystem 對照 >50%（現 10 列全在 E01，其餘 5 個生態系 0 成員）。"
     "門檻高低反映的是可得性不是重要性：confidence 每列都判得出來，"
     "strategic_priority 需要法說會等外部出處，本來就不可能全覆蓋。"
     "★**這些門檻是資料品質 KPI，不是分類規則的驗收硬門檻**——"
     "confidence 量的是歸屬判斷的信心（R018），不是資料可得性；"
     "若某批公司的年報品項就是不夠細、誠實的答案就是 C，"
     "**MUST NOT 為了讓填充率或平均分數達標而把 C 升成 B**。"
     "達不到門檻要寫進報告，不是改判資料。"),

    # The draft licensed "two A3 exposures", which reads as licence to count one
    # revenue line twice.
    ("R012", "Power × Compound 可交叉，但交叉的是節點不是比重",
     "SiC/GaN 同屬 Power Semiconductor 與 Compound Semiconductor，一家公司可同時掛兩個 A3 "
     "子產業曝險（與 R015 一致：製造模式與產品類型是兩個軸）。"
     "★但**同一條營收線的比重只能計在一個節點**（R004、R020②）——「可同掛兩格」講的是曝險列，"
     "不是比重可以複製兩份；另一格若無法獨立拆分，weight 必為 NULL。"
     "★更常見的錯誤是把**下游應用**寫成第二個曝險：「本產品應用於 Power MOSFET/IGBT」是客戶用途，"
     "不是該公司做了功率半導體製造，不得據以建立 A3-04。"
     "★現況：T25 Theme 目前 0 個成員，此交叉形態在庫裡尚無實例。"),

    # A term being hot does not mean the tree has no home for it.
    ("R010", "熱門名詞不各自獨立成 Theme",
     "AI Agent / LLM / GenAI / ML 歸入 T35 AI Software，不各自開一個 Theme。"
     "★T35 是**跨技術／產品概念的 Theme 集合，不取代 U6 的 Industry 歸屬**："
     "若樹上已有適當節點（如 U6 AI Software），依 R005 仍應建立 Industry Exposure，"
     "Industry 與 Theme 並存，不是二選一——不得把 AI Software 這門生意整個塞進 Theme 了事。"
     "★現況：T35 目前 0 個成員，U6 節點 0 筆曝險——這條規則描述的形態尚未在庫裡發生。"),

    # These three describe machinery that exists and is empty. Left unannotated
    # they read as descriptions of the data.
    ("R007", "Supply Chain 獨立於 Industry",
     "一家公司可有多個供應鏈位置；供應鏈位置與產業節點是兩個維度，不互相決定。"
     "落地欄位是 tw.company_industry_exposure.supply_chain_position（FK 指 "
     "tw.supply_chain_position 字典 16 項），描述該**曝險**的位置而非公司（見 R028）。"
     "★現況（2026-09-02）：字典 16 項＋層級 3 項已建，但 523 列**填充率 0**，"
     "「一家公司多個位置」至今 0 家驗證——本條目前是設計不是描述。"),

    ("R008", "Cycle 可多選",
     "Growth + Capex 等景氣屬性可並存，不是單選。"
     "★現況（2026-09-02）：tw.cycle_attributes 是 8 項純字典；公司層唯一載體 "
     "tw.company_profile.cycle_attributes 只有 7 家有值，且是逗號分隔的自由文字、無 FK，"
     "值與字典對不上（1605 寫 'Policy'，字典是 'Policy Driven 政策'）。"
     "447 家已分類公司中 440 家沒有任何 cycle 標註。多選機制尚未以關聯表落地。"),

    ("R009", "地區曝險與產業獨立",
     "Revenue 與 Production 的地區曝險分開標示，且與產業歸屬互不決定。"
     "★現況（2026-09-02）：tw.region_exposure_rules 12 列全是字典條文而非資料；"
     "公司層只有 tw.company_profile 的 7 家 × china_revenue／us_revenue 兩欄，"
     "字典宣告的地區只實作 2 個；生產地是自由文字 main_production_site（如「台灣/中國/泰國」），"
     "未分級。沒有 company_region_exposure 表——本條目前是設計不是描述。"),

    ("R013", "Group ≠ Theme",
     "Group 描述企業關係（母子/投資/關聯）；Theme 描述市場主題；分表存放。"
     "★group_type 僅限正式控制關係，**不得出現「正式集團 / 生態系」這種複合值**——"
     "生態系一律入 tw.ecosystem_master（R014）；GT04 台積電體系與 GT09 聯發科生態系"
     "原本就是這樣寫的，已改回單一值。"
     "★現況：business_group 20 列、company_group 81 列（成員由 tw.company_affiliations 推導，"
     "derived_from 留有持股佐證），是本節唯一有實資料的部分；company_group 無 known_date 欄，"
     "R021 的三軸不適用於集團層。"),

    ("R014", "Group ≠ Ecosystem",
     "Group＝正式集團/控制關係；Ecosystem＝公司與特定客戶、供應商、平台、聯盟或技術體系"
     "形成的關聯網絡（不限客戶/供應商，聯盟型如 E06 MIH 亦屬之）。"
     "★現況（2026-09-02）：ecosystem_master 6 列（E01 NVIDIA／E02 AMD／E03 Apple／"
     "E04 Tesla・EV／E05 雲端自研 ASIC／E06 MIH），但 company_ecosystem_mapping 只有 10 列"
     "且**全部指向 E01**，E02–E06 成員數皆 0；這 10 列的 confidence 全為 D、"
     "evidence_kind 全為 pending、known_date 全為 NULL（依 R021 對回測等同不存在）。"
     "本條目前是設計不是描述。"
     "★E01 的 coverage 是硬體供應鏈（GPU、AI Server、PCB、載板、電源、散熱…），"
     "不是 CUDA 那種軟體平台生態系——先前以 CUDA 舉例與庫裡的 E01 不符，已移除。"),

    # The 1-Core-per-company constraint lived only in a unique index. A rule
    # nobody wrote down is a rule the next schema change can drop.
    ("R029", "Core Cardinality：一家公司的 Core 最多 1 個",
     "**Core = 0 → Core pending**（合法狀態，見 R019）；**Core = 1 → valid**；"
     "**Core > 1 → validation error，載入器拒收**。"
     "★這不只是慣例，DB 層已強制：部分唯一索引 cie_one_current_core "
     "(stock_id) WHERE role='Core' AND effective_to IS NULL——索引不可延遲檢查，"
     "故換代時必須先降級舊 Core 再升級新 Core（db.seed_industry 為此把降級排在升級之前）。"
     "★**時間軸上同樣成立：任一時間點最多只有一個有效 Core**。歷史層可以有多次換代，"
     "但兩列 Core 的有效期間（`[effective_from, effective_to)`，端點相接不算重疊）不得交疊。"
     "★**`effective_to IS NULL` 表示該有效期間尚未設定終止日**，不是「在任何 as-of 都有效」——"
     "`effective_from=2025-01-01, effective_to=NULL` 的列在 as-of=2024-06-30 時**尚未生效**。"
     "**是否屬於某個 Data_As_Of 的有效期間，一律依 `[effective_from, effective_to)` 與 as-of 判定**："
     "`effective_from <= as_of AND (effective_to IS NULL OR as_of < effective_to)`。"
     "★區間互相比較（overlap）時，未終止的區間視為開放至無限遠"
     "（檢查以 `coalesce(effective_to, 'infinity')` 實作）："
     "故「2024-01-01～NULL」與「2025-01-01～2025-12-31」**是重疊**，必須被抓出來——"
     "這種錯誤現行層的唯一索引看不到，因為那兩列只有一列是現行的。"
     "否則「2024 年到底哪個是 Core」會有兩個答案。"
     "★唯一索引**只管現行層**，歷史層的交疊寫得進去，故由 db.check_industry 的 "
     "[core_overlap] 檢查；現況零交疊，**以該檢查當次輸出為準**。"
     "★**Core pending 的公司照樣可以建立 Industry Exposure、Theme、Supply Chain 與集團對照**"
     "——沒有 Core 只是主從未定，不是這家公司不能進資料庫；"
     "任何 `if no_core: reject_company()` 的實作都違反本條。"
     "★現況（2026-09-02）：447 家皆為 Core=1，pending 0 家、>1 者 0 家。"),

    # The three axes were defined but never constrained against each other.
    ("R030", "時間一致性：三軸之間必須互相說得通",
     "**必須成立**：① `known_date <= data as-of`——資料截止日之後才知道的事，"
     "在那個時點不存在；② `effective_from <= effective_to`；"
     "③ `evidence_period_end <= known_date`（**僅當 evidence_period_end 有值時**）；"
     "★**不具期間語意的 evidence，`evidence_period_end` 應為 NULL，不得以推測日期填補**——"
     "公司網站產品頁寫「目前產品包含 Touch Controller」沒有報導期間，"
     "填一個看起來合理的日期只是把「不知道」偽裝成「知道」；"
     "本欄的 NULL 是有意義的值（R028 同精神）。"
     "④ `evidence_period_end <= data as-of`。"
     "★**`known_date <= effective_to` 不要求成立，且不得設成約束**："
     "Effective 說的是「這個分類何時成立」，Known 說的是「我們何時才知道」，兩軸本來就可以錯開。"
     "某業務 effective 2020-01-01～2020-12-31、known_date 2021-03-15（FY2020 年報申報日）"
     "是完全合法的資料——2020 年這個業務確實存在，我們到 2021-03 才從年報知道。"
     "**Known_Date 晚於 Effective_To 時，該分類保留作歷史事實，但在任何 as-of < Known_Date 的"
     "回測中不得視為已知資料**——這正是 Known_Date 存在的目的。"
     "（migration 095 曾把它設成 CHECK cie_known_within_range，那是把兩軸讀成同一件事，"
     "已於 migration 096 移除；當時沒有觸發只是因為 effective_to 都是換代當日、"
     "known_date 都是過去的申報日，真正載入歷史層時就會被誤擋。）"
     "★**Data_As_Of 必須是明確宣告的查詢時間基準；可為當前資料庫快照日，"
     "或回測指定之歷史日期。任何未宣告的 as-of 不得用於資料篩選或回測。**"
     "互動查詢與 db.check_industry 取 `current_date`；回測取自己的時點。"
     "★回測的 as-of **不必是資料庫的某個歷史快照**——它做的是把當時可知的資料重新投影出來，"
     "所以 as-of=2024-06-30 的意思就是只放行 `known_date <= 2024-06-30`，如此而已。"
     "★四條分成兩種性質，這決定了它們能被什麼守住："
     "**②③ 是 row-level invariant**（只看這一列自己的欄位），故為 DB CHECK "
     "（cie_effective_range／cie_period_before_known，migration 095）；"
     "**①④ 是 query-time constraint**（要跟查詢上下文的 as-of 比），"
     "資料庫看不到那個值，做不成約束，故由 db.check_industry 的 [temporal] "
     "以 `current_date` 當 as-of 檢查——那是快照日這一種用法；"
     "回測那一種由回測自己的 as-of 過濾，規則管的是「必須有一個宣告過的值」。"
     "★[temporal] 同時覆蓋四條，這樣約束若被誰拿掉，會在檢查裡浮出來而不是無聲消失。"
     "★現況：全表（含歷史層）零違反，**以 [temporal] 當次輸出為準**——"
     "列數會變，寫死在規則裡就會過期（R027 已經踩過這個坑）。"),

    ("R022", "通路/代理商的產品線不得僅據產品名稱認定製造歸屬",
     "若公司**某一業務線**僅具通路/代理性質，其產品中即使出現設備、材料、載板、IC 等製造品項，"
     "也不得僅因產品名稱將該業務認定為製造業務；該產品如需保留產業關聯，"
     "應建立為 Indirect Industry Exposure，**不得因此建立製造型 Direct 或 Core**。"
     "★同一公司的其他業務線各自獨立判定，通路性質不外溢：若年報明確揭露自有製造、產能、"
     "製造設備或製造營收，該業務線照樣建立 Direct Exposure，不因公司另有通路業務而降級。例：蔚華科 V1 半導體通路 Core 93.77% Direct ＋ "
     "A2-13 測試/檢測設備 Indirect（代理的設備，比重已計在 V1）；"
     "港建 V1 Direct + 設備 Indirect。否則產業統計會把通路商錯算成製造商。"
     "判斷依據以年報為主：是否有佣金/代理性收入、產品線是否為具名製造品、正文有無自有產能描述。"
     "★官方分類僅作後驗參考不居優先（R002）——長華* 官方列電子通路業，但它的兩條線逐線看"
     "都是製造：IC 導線架 48.91%（A1-13）與封膠樹酯 29.29%（A1-12）皆為具名製造品且無佣金收入，"
     "故兩線皆 Direct；若以官方分類為首要依據會把整家公司誤判成通路。"),

    # An end-market node and a product node can both look right for the same
    # revenue line, and R004 only allows one of them.
    ("R031", "應用型 Industry Node 不得僅因下游應用存在而自動建立 Exposure",
     "應用型 Industry Node（例如 H4-01 車用IC）**只有在公司自身提供具有明確車用產品、車規能力、"
     "專用設計或車用業務實質的產品/服務時**，才可建立 Industry Exposure。"
     "僅因一般 IC、電子零組件或模組「被使用於汽車」，不得轉列為車用 Industry。"
     "同一業務曝險仍受 R004 約束，**不得因應用領域不同而對同一營收線重複歸屬**——"
     "「A4-10 網通 IC 100% ＋ H4-01 車用IC 100%」是同一個經濟活動被算兩次。"
     "★**H4-01 現況＝Definition Approved / Mapping Gate Pending，未啟用**（2026-09-03 裁定）："
     "有 node_code、有產品定義，但不得建立成員。整個 H 分支目前 24 個節點全數零成員、"
     "其中 12 個連定義都沒有，所以關著沒有損失什麼。"
     "★★**未決的是架構不是個案**：Industry tree 究竟只走「產品／價值鏈」單軸，"
     "還是允許「產品 × 終端應用」雙軸同時作 Industry leaf？"
     "若為前者，車用應另建 Application / End-Market Exposure 而不是 Industry leaf；"
     "若為後者，R004 的「同一業務只能一個 Industry」必須先重新定義得極精確。"
     "**在這題裁定之前，任何人都不得把公司掛上 H 分支**，也不得為了收某一家公司而回頭補寫"
     "G2 工業電腦、H3-03 Telematics 等空定義節點的定義——"
     "那會讓 taxonomy 被個案反向塑形（2388 威盛的 Mobile360／Firewatch／4D+ 系統業務即卡在此處）。"
     "★啟用時的義務：H4-01 一旦開放，**MUST 對全表重新掃描**（MCU/PMIC/Driver IC/Sensor IC/"
     "Connectivity IC 各廠皆受影響），不得只把觸發這次討論的那一家掛上去；"
     "掃描時逐家問 R025 的「無歧義涵蓋」，而不是看到「車用」兩個字就掛。"),
]

# The rules read as a list; grouped they read as a structure.
# What a rule is about (category) and what a reader does with it (kind) are two
# different questions. A hard constraint decides whether a row may exist, a
# decision rule needs judgement against evidence, and a validation rule reports
# and lets the load proceed — code that enforces the first and surfaces the third
# needs to tell them apart.
RULE_KINDS = {
    "hard_constraint": ["R003", "R004", "R011", "R016", "R018", "R020", "R029",
                        "R030", "R031"],
    "decision": ["R001", "R002", "R005", "R010", "R012", "R015", "R017", "R019",
                 "R022", "R023", "R024", "R025", "R026"],
    "validation": ["R006", "R007", "R008", "R009", "R013", "R014", "R021",
                   "R027", "R028"],
}

RULE_CATEGORIES = {
    "產業分類": ["R001", "R002", "R004", "R011", "R015", "R016", "R019",
             "R022", "R023", "R024", "R025", "R026", "R029", "R031"],
    "題材": ["R003", "R005", "R010", "R012", "R017"],
    "權重與證據": ["R006", "R018", "R020", "R027", "R028"],
    "供應鏈": ["R007"],
    "集團與生態系": ["R013", "R014"],
    "公司屬性": ["R008", "R009"],
    "時間": ["R021", "R030"],
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
    # What the tree looks like before this run, so that a node appearing and a
    # definition changing can be told apart from an unchanged upsert. R023 owes a
    # fresh Core adjudication for either (migration 098), and "someone will
    # remember" is what that queue exists to replace.
    cur.execute("SELECT node_code, main_products FROM tw.industry_tree")
    was = {r["node_code"]: r["main_products"] for r in cur.fetchall()}

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

    # A node-level row means the whole table still owes a scan against that node;
    # who is affected is not knowable here, only that someone has to look.
    for code, _parent, _level, _name, products in NODES:
        if code not in was:
            _raise_reassessment(cur, code, "node_created",
                                "節點新增，全表尚未依 R025 檢查有無其他公司應掛此格")
        elif (was[code] or "") != (products or ""):
            _raise_reassessment(cur, code, "definition_changed",
                                "Product Definition 變更，全表尚未重新檢查候選集合")

    cur.execute("SELECT node_code FROM tw.industry_tree")
    stale = [r["node_code"] for r in cur.fetchall() if r["node_code"] not in {n[0] for n in NODES}]
    if stale:
        # Deepest first: an L2 cannot go before its children.
        cur.execute("DELETE FROM tw.industry_tree WHERE node_code = ANY(%s)", (stale,))
    return len(NODES), len(stale)


def _raise_reassessment(cur, node_code, kind, note):
    """Open a queue item unless the same one is already open (migration 098)."""
    cur.execute("""
        INSERT INTO tw.core_reassessment (stock_id, trigger_kind, trigger_node, note)
        SELECT NULL, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM tw.core_reassessment
            WHERE stock_id IS NULL AND trigger_node = %s
              AND trigger_kind = %s AND resolved_on IS NULL)
    """, (kind, node_code, note, node_code, kind))


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

    rule_kind = {rid: k for k, ids in RULE_KINDS.items() for rid in ids}
    for rule_id, kind in rule_kind.items():
        cur.execute("UPDATE tw.classification_rules SET rule_kind = %s WHERE rule_id = %s",
                    (kind, rule_id))

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
