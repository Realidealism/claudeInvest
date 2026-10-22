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
    ("R004", "同一經濟活動僅能有一個 Industry 歸屬",
     "公司可同時存在多個 Industry Exposure；不同經濟活動可分別歸屬不同 Industry 節點，"
     "但**同一經濟活動不得重複計入多個節點**。例：欣興的 ABF 載板歸 B3-01、硬板歸 B2-01，兩者並存；"
     "但同一條年報營收線不得同時在兩個節點計入比重（見 R020）。"
     "★**判準是經濟活動，不是營收線**（2026-09-08 措辭修正）：營收線只是判斷"
     "兩件事是不是同一個經濟活動的**證據之一**，兩者不等價。一條寫著「電子產品 60%」"
     "的營收線底下可能是 IC、模組與服務三種活動；反過來，"
     "同一個經濟活動也可能被年報拆成兩條線揭露（R020④ 的合計即為此形）。"
     "★所以本條禁止的是**同一活動被算兩次**；一條粗營收線橫跨多個節點時不是本條的違規，"
     "而是 R020② 的 weight=NULL 情形——依 R020 ②-MX，"
     "證據支持且可歸屬的節點建列，只是誰都不得認領那個比重；同一混合營收線分在多個節點建列，"
     "本身不是 Indirect 的理由（R018、R020③）。"
     "三者的分工：本條管同一活動不得重複計入；"
     "R020 ②-MX 管混合營收線何時成立、哪些節點建列；"
     "R020 其餘各項（含 ②-MX (7)）管數字怎麼放。"),

    # "新產品" was too wide: a product can be new and still have a home.
    ("R005", "尚未有適當 Industry 節點的新興技術/產品優先進入 Theme",
     "若技術／產品尚無合適 Industry Node，**於新增節點前**可暫以 Theme 表達其市場概念；"
     "一旦建立對應節點，仍依 R025 建立 Industry Exposure。"
     "★Theme 是**暫時性／跨產業的表達**，Industry 才是最終歸屬——"
     "本條不是 R025 的替代路徑：「找不到節點就丟 Theme」是誤讀，R025 要的是去新增節點。"
     "若已有適當節點，Industry 與 Theme 可並存，不必二選一。"
     "HBM 即為此例：Theme T07 與該公司依其實際價值鏈角色所歸屬的 Industry Exposure 同時成"
     "立（見 R017）。★原稿列舉的 A5-04 HBM Packaging 與 A6-07 HBM Test 皆非現行"
     "節點——A6-07 已移出本樹改由 T07 表達（見 A6 定義），A5-04 從未存在，曝險表無任何歷史列。**此"
     "處不改列另一組節點代碼**：HBM 是測試條件與終端技術、不是產品或服務類別，落點須逐案依價值鏈角色判定，把節點列舉"
     "寫進規則正文只會再次腐化（R027）。★**這條規則說的是允許並存，不是斷言已經並存**：T07 的成員數與各相關節"
     "點的曝險數以當次查詢為準，不寫進正文（理由見 R027）；"
     "無論並存形態是否已出現，本條的規範內容都不變。★節點一律引用 node_code，不引用「A3 Memory」這類非節點的口語名——"
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
     "★**這段刻度目前是待建欄位的規格**：上面三個範例是刻度的示範而非庫裡的實例。"
     "confidence 的填充率以當次查詢為準，不寫進正文（理由見 R027）；"
     "只要該欄仍是空的，刻度就還沒有被行使過。"
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
     "`exposure_type='Direct'` 的**葉節點**（R016），且該曝險的**業務狀態不是 Emerging**（規範定義），"
     "先依相同葉節點聚合可可靠權重，即為 Core Candidate Set。"
     "★**候選資格 MUST NOT 依賴既有的 role='Core'／'Major'／'Minor' 判定結果**——"
     "Core 是本規則的**產物**，拿它當輸入就形成自我增強的迴路："
     "上次是 Core → 這次仍有資格 → 這次又是 Core。同理，「不逐檔手填 core_industry」"
     "若改成從 role 餵回來，只是把手動維護換了個入口。"
     "★唯一可用的資格條件是**業務狀態**：依 R026 的定義，尚無可辨識營收且僅有布局 evidence "
     "者為 Emerging，不進候選；其餘皆進。★★**這條資格條件的現在與未來落地方式不同，實作時必須分清楚**：**現階段** schema 尚無 `business_status` 欄，Emerging 已由 `cie_role_is_a_ranking` CHECK 在 DB 層禁止，因此候選集合的實作**無需另行過濾**——上面那句是規範定義，不是現在要寫的 WHERE 條件。**未來**新增 `business_status` 後，**MUST 以該欄位排除 Emerging，不得重新用 role 表示狀態**。★此判定應回到 R026 的兩個條件，"
     "不得只讀 role 欄位的字面值**。"
     "★★`role` 這一欄曾同時裝兩種東西——Core／Major／Minor 是**推導結果**，"
     "Emerging 是**業務狀態事實**。**2026-09-08 已由 migration 103 加上 "
     "`cie_role_is_a_ranking` CHECK 禁止 role='Emerging'**，"
     "role 自此只承載排名。enum 值未移除、也未新增 business_status 欄，理由各自明確："
     "移除 enum 值要連帶重建 tw.v_company_industry_exposure 等依賴 view，"
     "而該改動不動任何一列資料；新增 business_status 欄則會在現行每一列上得到一個"
     "恆為 Operating 的常數欄，描述不了任何事實。"
     "★**未來出現第一個真實 Emerging 案例時的正確動作**：新增 business_status 欄"
     "並解除該 CHECK，**不得把狀態寫回 role**。現在禁止它，正是為了讓那一天不必先清理錯資料。"
     "★承上，候選集合的實作因此可簡化為不必再過濾 Emerging；"
     "既有的 `role IS DISTINCT FROM 'Emerging'` 保留為防禦性條件，"
     "若日後 CHECK 被解除它會自動恢復作用。"
     "★**同一公司、同一 Leaf Node 只能形成一個候選項——候選集合以 `(stock_id, node_code)` 去重**。"
     "公司有 A5-05 30%、A5-05 20%、A6-01 40% 三列時，候選是 A5-05=50% 與 A6-0"
     "1=40% 兩項，"
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
     "★★**Core Decision Gate（2026-09-08 增訂，把裁定寫成可機械檢查的三分支）**："
     "**(1) 候選集合只有一項** → 該項即 Core。"
     "**(2) 多項而依 R023 的證據階層能產生唯一勝者** → 該項為 Core，"
     "並依 R023 記錄哪一層做了決定性工作。"
     "**(3) 多項而無法排除合理的競爭解釋** → **Core Pending**：移除該公司的 Core 列"
     "（該曝險改記其他 role），理由寫進留下來的最大那一列。"
     "**(4) 候選集合被 taxonomy 截斷** → **Core Pending**（2026-09-08 增訂）："
     "當公司**最大且可可靠辨識的營收／業務線**（何謂可靠依 R020——必須是公司自身的營收，產業產值、市場規模、市佔率與損益表科目皆不算）**在樹上沒有任何節點可歸屬**，而現有候選只是它較小的業務時，"
     "分支 (1) 的「候選只有一項故該項即 Core」**不適用**——那個恆等式的前提是"
     "候選集合已窮盡該公司的可歸屬業務，taxonomy 有缺口時這個前提不成立。"
     "處理方式與 (3) 相同：不建 Core 列，role 照 R026 的數值預設判定，"
     "理由寫進留下來的曝險，並在 db/TAXONOMY_GAPS.md 開 gap。"
     "★**判定條件（三者皆須成立，缺一即回到 (1)）**：(a) 該無節點營收線經 evidence 確認其經濟內容"
     "（不是「其他」這類未揭露欄——**未揭露不等於無節點**，那是 evidence coverage gap，見 4563 百德）；"
     "(b) 它大於現有候選中最大的一個；(c) 全樹確認無可收之節點，且已開 gap 記錄。"
     "★**兩個建立本分支的實例**（2026-09-08 G2 mapping）：2399 映泰電腦主機板 49.00%＋顯示卡 16.00% "
     "全樹無節點，只有工業電腦 11.00% 落在 G2——照 (1) 會得到「65% 的業務不在候選集合內，"
     "卻由 11% 的線當 Core」；5484 慧友監控三線合計 61.53% 全樹無節點，只有工業電腦 26.16% 落在 G2。"
     "兩家的 gap 分別記為 G17 消費級 PC 主機板與顯示卡、G18 監控攝影機與錄影系統。"
     "★**本分支與 R026 分開**：role 可以照既有候選的比重判定（映泰 11% → Minor、慧友 26.16% → Major），"
     "因為 R026 問的是「這門業務對這家公司相對重要嗎」，那個問題在 taxonomy 有缺口時仍答得出來；"
     "Core 問的是「這家公司的產業定位是什麼」，那個問題答不出來就不該編一個答案。"
     "★**gap 補上節點後 MUST 重跑本公司 Core**（與 R023 的 taxonomy 變動條款同一條路徑）。"
     "★**分支 (3) 禁止使用下列 tie-breaker**：研究員主觀偏好、公司名稱或其歷史印象、"
     "營收最大的那一項、官方產業分類。"
     "**若這四者能決定，代表證據階層本身沒有分出勝負，那就是 (3) 而不是 (2)。**"
     "沒有這條禁令，Core Pending 這個狀態會存在但永遠不被使用——"
     "reviewer 總能找到一個看起來合理的理由硬選一個。"
     "**「不手動維護」指的是不另立一個手填的 core_industry 欄位**，"
     "不是說這一步可以自動算出來——R023 的 ①～⑤ 本來就需要人對證據做判斷。"
     "★**Core 的實況是「裁定後記錄」不是「計算」**：role 目前由 db/industry_data.py 逐列鍵入，"
     "載入器與 db.check_industry **驗證**它符合規則，而不是產生它。"
     "把這件事寫清楚，是為了不讓標題變成一個沒有實作在後面的宣稱（R006 的 Profit 同型）。"
     "★**候選只有一個時 Core 是恆等式**，不需要理由；"
     "**候選有兩個以上時 R023 才真的被行使過**，該次裁定的理由 MUST 寫進 "
     "Core 那一列的 evidence，以「Core 理由：」起頭——沒有記錄理由的多候選裁定，"
     "由 db.check_industry 的 [core_reason] 列出未記錄者。"
     "★候選集合本身已實作為 `db/core_candidates.py`（`python -m db.core_candidates`），"
     "四個謂詞逐條對應本規則、支援 `--as-of` 重算歷史時點；[core_reason] 直接呼叫它，"
     "候選集合只有一份實作。它同時檢出 Core 不在自己候選集內的情形"
     "（Indirect、掛在分支節點、或候選集為空），那是其他檢查看不到的。"
     "★**多個業務群之間如何取捨見 R023**——本條的兩個例子"
     "（精材 50.16 > 47.86、中石化 J9 46.81 > J1-01 40.14）最後都落在較大的那一群，"
     "那是判定的結果不是判準；義隆的 C11-01 69.92% 就沒有拿到 Core。"
     "★只有確實屬於同一 Industry node 的業務線才能聚合，**不得因上層產業、供應鏈或終端應用相近"
     "而跨節點合併**，亦不得跨節點為取得較大比重而合併："
     "A5 封裝 45% 與 A6 測試 48% 不可併成「半導體 93%」再據以定 Core，那等於把 taxonomy 拆掉；"
     "中石化的己內醯胺及其附屬產品 46.81%（J9）與丙烯腈及其附屬產品 40.14%（J1-01）分屬兩節點，"
     "不得合併成「石化 87%」；本例依公司主要產業定位判定 Core 為 J9，"
     "而非因兩者合計後的 87% 得出。"
     "★本例的數字 2026-09-03 更正過：原寫 50.157／49.445，那是年報另一張**公噸**單位的"
     "產銷量表被當成營業比重存進 tw.product_revenue 後重新正規化的結果（三列合計僅實際營收的 "
     "2.1%），營業比重表的真值是 46.81／40.14／鄰苯基苯酚 0.77／其他部門 12.28。"
     "結論（Core 在 J9）不變，論據已換；db.check_industry 的 [scale] 就是為了讓"
     "這類「單位錯了但比例看起來很正常」的資料浮出來而加的。"
     "例：同一公司的多條營收線經裁定歸入同一 node 後，應先按 node 聚合，再與其他候選 node 比較；本例示範"
     "的是「同 node 聚合後再裁定」，不是某個特定節點代碼。該公司現行的 Core 節點、weight 與組成線一律以"
     "當次查詢為準（R027）。"
     "★若無法唯一判定（如 42%／38%／20% 三群資料完整但商業本質分不出主從），"
     "該公司視為 **Core pending**：資料庫**不建立 Core row**，由 db.check_industry 的 [core] "
     "列為 validation finding；載入器不因此拒收。"
     "★Core pending 是資料的狀態不是欄位——schema 沒有 core_status，也不需要加。"
     "★不得改以 role=NULL 表示（role 是列上的 enum，沒有 NULL 這個值），"
     "更不得因為要湊出一個 Core 而隨便挑一群。**沒有 Core 不等於沒有主要產業**，"
     "只是這家公司的主從還沒判定。"
     "★此例的節點合計必須真的落地（R020④）：該 node 的 `weight_item` 必須列出它實際聚合的每一條"
     "線，`weight_pct` 必須是那些線的合計；若只存其中最大的一條，就無法正確表達該 node 的聚合曝險。具體"
     "的組成與數字以當次查詢為準（R027）。"
     "★Core ≠ Revenue Exposure Level：超豐無比重仍為 Core。前者是產業定位，後者是營收分箱。"
     "★**多數公司只有一筆曝險，三段式在那些公司上是恆等式**，真正有裁定作用的是多曝險那批；"
     "各族群的家數、以及 Core=0 與 core pending 的件數，一律以 db.check_industry 的 "
     "[core]／[core_pending] 與 `db.core_candidates` 當次輸出為準，不寫進正文（理由見 R027）。"
     "★**Core pending 不是異常**：新增節點或改寫節點定義都會依 R025 觸發一批待重查的候選集合，"
     "看到 [core_pending] 非零是預期中的工單，不是資料損壞。"),

    ("R020", "Weight 可為 NULL，且必須標明類型與分母",
     "① weight_pct 表示該 Industry 節點可歸屬之營收曝險，須標示 weight_type／weight_basis，"
     "不得將不同分母或不同層級的數字混用——47.86% 是營收占比、30% 可能是產能或策略描述，"
     "無此二者的裸數字不可比較；未揭露時 weight_pct=NULL，不得臆測百分比。"
     "★★**①-C｜產業統計不是公司營收**（2026-09-09 增訂，由 6141 柏承建立）："
     "**產業產值、市場規模、市場佔有率、成長率、同業比較、公司在市場中的地位，"
     "以及損益表科目，一律不得作為公司自身的營收品項**，因此不得成為 weight_item，"
     "也不得出現在 evidence 的比重引述裡。"
     "★這條不是「解析器的事」——它是 **evidence provenance 的防火牆**。"
     "柏承的年報產業概況章有一張全球 PCB 產值表，舊解析器把它當成營業比重表讀進 "
     "tw.product_revenue，於是「全球PCB銷售產值」成為該公司唯一的營收線；"
     "它接著成為 B2-01 曝險的 weight_item，Core evidence 又照抄成"
     "「年報營業比重表：全球PCB銷售產值 100.00%」——一個 100% 的 Core weight "
     "建立在一條不存在的營收線上，而**分類層的每一個檢查都把營收表當成既定事實**，"
     "所以沒有任何一個看得到它。污染鏈是：解析 → item → weight_item → evidence → Core。"
     "★機制層面已於同日落地：`scrapers.annual_report_revenue` 的 `_is_context_item()` "
     "在所有 reader（table／lines／prose）之前先問「這段是不是營收揭露」，"
     "整張表多數列屬 context 時整張拒收（8401 白紗科的市佔表）；"
     "回歸案例固定在 `tests/test_revenue_context_gate.py`。"
     "★**寫 evidence 時的自檢**：引用一個比重數字前，先問這個數字的分母是"
     "**這家公司的營收**，還是**這個產業的規模**。兩者在年報裡常常只隔一行。"
     "★★★**①-S｜Weight 的狀態模型（2026-09-09 訂立；2026-09-17 增為七個狀態，分屬兩個 namespace）**："
     "**A. Weight Availability——這一格的比重拿不拿得到、為什麼拿不到**："
     "　`WEIGHTED` 有可用的比重（**不等於 weight_pct 不是 NULL**，見 ①-W）；"
     "　`TOO_COARSE` 公司**確實沿產品／業務軸揭露**了比重，但粒度比本樹粗，該線同時涵蓋本節點與其他節點而年報未給拆分依據——等公司揭露變細就會有數字。★該「其他節點」**必須能具名**（R2，2026-09-17 裁定）；指不出是哪一個節點時不得用本值，見 GRANULARITY_INSUFFICIENT；　`GRANULARITY_INSUFFICIENT` 公司**確實沿產品／業務軸揭露**了比重，但以現有揭露的粒度**目前**無法可靠計算本節點的比重，**且無法依 R2 具名該線同時涵蓋的另一個節點**（2026-09-17 裁定新增；DB 值 `Granularity_Insufficient`）。它說的是「以現有證據目前算不出」，**不是**「本節點永遠不可有比重」。**非終態**，收斂條件二擇一：(a) 定義或 mapping 修到可以可靠歸屬——轉 WEIGHTED，或另一節點可具名後轉 TOO_COARSE；(b) 來源新增可分辨的營收拆分——轉 WEIGHTED。兩者都未發生前不得僅因先前已是本值就繼續停留。★本值描述的是**這一筆的比重**；指不出另一節點若是因為樹上沒有格，該缺口仍須依 ①-A (3)／R025 另行登記，本值不取代 gap 紀錄。機制：db.check_industry 的 [granularity_queue] 列出所有本值的列；"
     "　`NOT_DISCLOSED` 公司**根本沒有沿產品軸揭露**（比重表的軸是會計科目、交易性質、應用別、地區別或行業別）——這不是粒度問題，修解析器也拿不到，是**終態**；"
     "　`NOT_EXTRACTED` 來源存在或應存在而我們沒有成功取出——**非終態，見 R020-NE**；"
     "　`DISCLOSURE_INVALID` 公司揭露了數字而該數字經驗證不可信（4721 美琪瑪兩條線 59%＋51%＝110%，比例欄本身錯誤）。"
     "**B. Counting Semantics——這一格本來就不該有獨立比重**："
     "　`NOT_APPLICABLE` 該曝險的營收**已由另一個節點計入**，在此再給比重就是市場層的重複統計。"
     "★**兩個 namespace 不可互換**：A 描述「來源給了什麼」，B 描述「計數架構允許什麼」。把 NOT_APPLICABLE 放進 A，就會有人拿它處理「查不到」「解析壞了」「揭露太粗」——那正是這一欄要防止的事。"
     "★**分辨 TOO_COARSE 與 NOT_DISCLOSED 的操作判準是「公司選了哪一條軸」，不是「數字夠不夠細」**。4958 台燿「各式印刷電路板 100%」與 8046 南電「印刷電路板製造及銷售 97.23%」是產品軸太粗；6291／6415／6799「年報按應用別揭露」、6237 驊訊「銷貨收入／勞務收入／權利金」、3583 辛耘「比重表僅列會計科目」是根本沒有產品軸。兩者的後續動作不同，這就是分開的理由。★**分辨 TOO_COARSE 與 GRANULARITY_INSUFFICIENT 的判準是「另一個節點能不能具名」，同樣不是「數字夠不夠細」**。兩者不是同義詞：TOO_COARSE 在年報揭露變細之前可以是穩定狀態（② 乙型「證據不足以拆分時應永久保留」），GRANULARITY_INSUFFICIENT 則是非終態，必須追蹤它的收斂條件。"
     "★★**R020-NE｜NOT_EXTRACTED 是修復狀態，不是分類結論**：它的意思是「來源存在或應存在，但系統尚未成功取得比重證據」，**不得**讀成「公司沒有揭露」。extraction／retrieval 修好之後，該列 **MUST** 重新裁定為 WEIGHTED／TOO_COARSE／GRANULARITY_INSUFFICIENT／NOT_DISCLOSED／DISCLOSURE_INVALID（或在計數語意獨立要求時為 NOT_APPLICABLE）。**一列不得僅因為先前被標為 NOT_EXTRACTED 就繼續停留在該值。**"
     "2026-09-09 建立時的 21 列全部有具名的失敗原因：3006 晶豪科的「占營業比率為 100%」讀不到、5347 世界的「晶圓收入 46,763,229」被 (cid:3) 佔位符切斷、4989 榮科的數字抽成「����,���」、2355 敬鵬與 6789 采鈺的 narrative 抓錯章節、4923 力士與 6425 易發連年報都沒抓到。把這 21 列標成 NOT_DISCLOSED，會把**待修工單偽裝成公司的行為**。機制：db.check_industry 的 [weight_status_lifecycle] 在來源已可取得時把它們報出來。"
     "★★**①-A｜四種「沒有」是四件不同的事，不可互相代用**（2026-09-10 訂立，由 4989 榮科／5347 世界／4764 雙鍵三例逼出）："
     "　**(1) 讀不到**（`NOT_EXTRACTED`）——來源存在，抽取／取得失敗。這是**工單**，屬 parser、字型編碼或 retrieval 層，與公司無關，也與樹無關。"
     "　**(2) 沒揭露**（`NOT_DISCLOSED`）——來源存在且讀得到，公司沒有沿產品軸給數字。這是**終態**，是公司揭露行為的事實。"
     "　**(3) 樹上沒有格**（taxonomy gap，見 db/TAXONOMY_GAPS.md）——業務真實存在且讀得到，但沒有節點能收它。這是**本樹的缺口**，與能不能拿到數字完全無關（4585 達明的自動化整合 15.40% 就是：數字清清楚楚，沒有家）。"
     "　**(4) 日期缺**（[known_missing]／[known_unavailable]）——那兩個檢查問的是**證據的日期**，不是比重，兩者不同軸，任何時候都不該互相引用。"
     "★把 (1) 說成 (2) 會**把待修工單偽裝成公司的行為**；把 (3) 說成 (1) 會讓人去修 parser 而不是去建節點；把 (2) 說成 (3) 會憑空生出一個沒有母體的 gap。三種誤標都會讓後續的人做錯事，而且都不會觸發任何檢查——所以判定時先問「這一格的『沒有』是哪一種」，再問它的值。"
     "★實例對照（同一天、三種不同的『沒有』）：4989 榮科的年報**有**比重表、中文正常、**只有數字字符沒有 Unicode 對照**——是 (1) 不是 (2)，換 parser 無效、需 OCR；1722 台肥的年報四個關鍵詞零命中——是 (2)，修任何工具都不會出現數字；4585 達明的 15.40% 讀得清清楚楚——是 (3)，要建的是節點不是解析器。"
     "★★**①-W｜WEIGHTED 的四個條件，缺一不可**：(a) weight_pct 可信；(b) weight_item 已具名、可回溯到年報的哪幾條線；(c) evidence 支持該筆 mapping；(d) 計數語意未要求 NOT_APPLICABLE。**「weight_pct 不是 NULL」不足以單獨證明 WEIGHTED**——沒有這一條，任何人手動填一個數字就能讓所有檢查把它當成完整的比重，6141 柏承的「全球PCB銷售產值 100%」正是那樣活了好幾週（見 ①-C）。機制：[weight_status_consistency] 檢查 (b)，[stale] 檢查它的可回溯性。"
     "② 同一營收線不得在多個 Industry 節點重複計重。"
     "若單一年報營收線涵蓋多個節點但無法可靠拆分（② 甲型 taxonomy collision 除外，"
     "依 R031），依 ②-MX 判定：證據支持且依正式定義可歸屬的節點原則上應建列，"
     "exposure_type 依 R018、R022、③ 判定，"
     "各列 weight_pct=NULL，"
     "weight_status 依 ②-MX (7)（TOO_COARSE 或 GRANULARITY_INSUFFICIENT）；"
     "不得將該上層營收線比重套用至任一子節點。"
     "例：欣興「印刷電路板 99.07%」橫跨 B3-01 ABF 載板與 B2-01 硬板、"
     "德律「自動檢測設備 97.21%」橫跨 A2-13 測試/檢測設備（半導體側）與 B5-01 製程設備（PCBA 側），"
     "兩對都是兩格 Direct、兩格 NULL——**包括原本掛著比重的那一格**；"
     "台化「化工一部 39%」同理，部門名稱不指明產品，不可當成任一細格的比重。★★**②的 NULL 有兩種來源，只有一種該消失，必須分開記：**(甲) **taxonomy collision 型**——同一產品同時滿足兩個 sibling node 的定義，因為那兩個定義用了不同的分類軸。這是**樹的缺陷**，依 R031 修好 parent boundary 後**應該消失**；"
     "(乙) **揭露粒度型（weight_status 依 ②-MX (7)：TOO_COARSE 或 GRANULARITY_INSUFFICIENT）**——節點定義已足以唯一區分（或線內含已正式揭露、節點未裁定的組成，"
     "見 ②-MX (2)(b)），但公司真的同時做兩邊，而年報只給一條合併營收線。"
     "這是**來源揭露的限制**，修樹不會讓它消失。**記 TOO_COARSE 者，"
     "證據不足以拆分時應永久保留**；"
     "記 GRANULARITY_INSUFFICIENT 者不是終態，未定組成的節點歸屬裁定後，"
     "依 ①-S 的收斂條件重新判斷。本型的操作細則見 ②-MX；② 甲型不適用 ②-MX。"
     "★★★**taxonomy collision 不是一種 weight_status，是 taxonomy QA finding**：`weight_status` 的每個值回答的都是「**這家公司這一筆**的比重怎麼了」（TOO_COARSE／GRANULARITY_INSUFFICIENT／NOT_DISCLOSED／NOT_EXTRACTED／DISCLOSURE_INVALID 皆然），而 collision 說的是「**我們的樹**有問題」——主體不同，不得共用同一欄。**MUST NOT 為 collision 新增 weight_status 值**，否則該欄會從「缺席原因」退化成什麼都能塞的欄。collision 的正確去處是 R031 的 taxonomy 治理路徑（修 parent boundary、在 db/TAXONOMY_GAPS.md 留紀錄），而該列在修好之前**照樣要有一個屬於它自己的 weight_status**——通常是 TOO_COARSE，因為在樹修好前那條線確實無法唯一指派。★由此得到四層各司其職：`weight_pct`（數字）→ `weight_status`（沒有數字的原因）→ `evidence`／`evidence_kind`（依據什麼、什麼類型）→ taxonomy QA（樹本身的缺陷）。"
     "★判斷順序：先問「這兩格的定義是不是用了不同分類軸而重疊」——是就修樹(R031)，"
     "不要用 NULL 把重疊蓋掉；否則再問「年報有沒有可靠的拆分依據」——有就分配，"
     "沒有就依 ②-MX 判定建列，weight=NULL 並依 ②-MX (7) 記 weight_status。"
     "★★★**NULL 本身不是錯誤狀態，沒有正確原因的 NULL 才是。★2026-09-08 起這句話有了 schema 支撐：`weight_status` 欄（migration 102）記錄**為什麼沒有數字**，值為 Too_Coarse／Not_Disclosed／Not_Applicable，weight 存在時本欄為 NULL（沒有缺席可解釋）。（歷史註記：上句記錄的是 2026-09-08 建欄當日的值。其後 migration 105（2026-09-09）增補 Not_Extracted、Disclosure_Invalid；2026-09-17 裁定新增 Granularity_Insufficient，使 ①-S 的狀態模型由六種擴為七種〔DB 列舉值由五個擴為六個，WEIGHTED 不佔列舉值〕。此次新增補足「已有可辨識的產品／業務軸，但現有揭露粒度不足以計算權重，且無法具名另一節點」之狀態，不改變既有六種狀態的既定語義。）★★**本欄與 evidence_kind 是兩個問題，且 MUST NOT 共用命名空間**：後者答「這個判斷根據什麼類型的證據」（`annual_report_table`／`annual_report_prose`／`company_site`／`investor_conf`——**只有這四個值**），前者答「所以為什麼沒有比重」。**evidence_kind MUST NOT 承載任何原因**：`filing_too_coarse`／`no_filing`／`pending` 三個舊值都是原因不是證據類型，已於 2026-09-10 由 CHECK 移除（migration 107），該日之前的 132 列依其 evidence 原文各自還原為 table 或 prose。`filing_too_coarse` 之所以曾經看起來夠用，只是因為它剛好同時是兩者的答案——而 2026-09-08 建欄當日，225 列 NULL 中有 **94 列的證據類型是 annual_report_prose／company_site／annual_report_table／investor_conf，完全沒有記錄原因**。那批列不由 migration 補值（補了就是編的），改由 [weight_reason] 逐列報出——**它只會因為逐家讀年報裁定而下降，不會因為冒出比重而下降**；還剩幾列以該檢查當次輸出為準。★本欄刻意**沒有** Quantified 值（有數字的列沒有缺席要解釋），也刻意不收 Indirect 與 Emerging——那兩個分別是 exposure_type 與業務狀態的值，放進來就重演 role 曾有的病：一欄裝兩種事實。**把 69.86% 硬拆成 A2-05 40%／A2-12 29.86% 會讓 coverage 好看，但那兩個數字是編的——coverage 上升而資料品質下降。所以該追蹤的不是「NULL 有沒有變少」，而是「NULL 的原因對不對」。★實例（2026-09-04，同一批列在修樹前後換了原因而沒有消失，可當回歸樣本）：3131 弘塑 69.86%、3581 博磊 65.07%、3583 辛耘 41.90%。修樹前是 A2-05 清洗設備（機台類型）與 A2-12 封裝設備（製程階段）的定義重疊(甲型)；A2 邊界於該日釐清後三家**仍為 NULL**，但原因換成「公司真的前後段都做、年報只給一條線」(乙型)。資料列不必消失，要轉換的是 reason 與 evidence。"
     "★★**②-BU｜事業群／事業處級營收線**（2026-09-08 增訂，"
     "由 6166 凌華與 6414 樺漢建立）：營收線若以**事業群、事業處或部門**為單位揭露，"
     "且其自述內容涵蓋兩個以上 Industry role，而年報無可靠拆分基礎，"
     "則**整條營收線不得以其中任一產品或事業描述推導 node weight**；"
     "**但該線仍可作為 Core／strategic identity 的質性證據**，"
     "前提是 R023 的獨立證據足以支持該公司之核心經濟定位。"
     "是否構成混合線依 ②-MX (2)(3) 判定；"
     "本項只補充事業群、事業處或部門線的權重限制與質性證據用途。"
     "質性證據用途不得成為把非產品軸或事業群百分比推導為產品節點權重的理由。"
     "★這一條區分的是**量化主張與質性主張**：前者問「這條線值多少比重」，後者問"
     "「這家公司的產業定位是什麼」，兩者的證據門檻不同，一條線可以支持後者而不支持前者。"
     "沒有這個區分，會出現兩種相反的錯誤：把混合線的數字當成單一節點的 weight（灌水），"
     "或因為拿不到數字就連 Core 都不敢給（把證據不足誤讀成證據相反）。"
     "★實例：6166 凌華七條事業處線中，模組化電腦與客製化強固電腦兩條的產品列全屬 G2 故計入(48.00%)，"
     "顯示運算（含博弈電腦、醫療顯示、繪圖卡）、邊緣運算平台（含 ROS2／AMR 控制器、工業級 SSD）、"
     "IoT 策略解決方案（含 PXI 量測平台、運動控制卡）三條混合故整條 NULL；"
     "6414 樺漢「工業物聯網事業群 48.13%」橫跨工業電腦與網安通訊，weight 為 NULL，"
     "但其 Core 仍由產業概況的 IPC 本業自述支撐(R023①④)。"
     "★**判斷順序**：先問這條線的**內容**是否落在單一節點（不是問它的**名稱**——"
     "4562 穎漢的「全自動整廠設備」名稱指向別支而內容是彎管機，仍是單一節點）；"
     "內容跨節點才適用本條。"
     "★★**②-MX｜混合營收線（2026-10-16 增訂）**：本項是 ② 乙型（揭露粒度型）的操作細則。"
     "② 甲型（taxonomy collision）依 R031 修樹，不適用本項；"
     "同一營收線同時含 collision 產品與其他產品時，"
     "collision 那項依 R031 與 ② 甲型處理，其餘產品仍依本項判斷，"
     "不因部分產品適用 R031 就把整條線排除於本項之外，"
     "也不得以本項的建列結果蓋掉 collision 的修樹追蹤。"
     "**(1) 判斷單位**：營業比重表或同等正式揭露中的**單一營收線**。"
     "公司同時經營多類產品，單憑這一點不構成混合線。"
     "**(2) 觸發條件**：同一營收線的實際內容 (a) 跨兩個以上節點，"
     "或 (b) 同時含已知可歸屬的產品與已正式揭露在該線內、但節點歸屬尚未裁定或無節點可歸的組成，"
     "且年報未提供可驗證的分項金額或比例。線內產品都屬同一節點時不觸發，依 ④ 合計。"
     "只在年報其他段落提到某產品、未證明它在該線內，不構成 (b)。"
     "**(3) 線內容證據**：觸發須有下列兩種證據之一——**MX-NAME（線名證據）**：營收線名稱本身列出跨節點的產品；"
     "**MX-MAP（對應證據）**：年報明確把產品對應到該營收線（產品用途表、營業比重表附註、業務內容逐線說明等）。"
     "**MX-CLUE（線索）**——只有產品清單、官網分類、產能描述而未對應到該線——不得單獨觸發，"
     "只作調查線索，該線維持待補證。MX-NAME 與 MX-MAP 是證據類型，不是強度排序；"
     "達到觸發門檻，不等於已證明線內產品符合各節點的正式定義，也不等於已完成建列裁定。"
     "營收線佔營收比重高、其餘線名未提到某產品、或年報主要產品章節列有該產品，"
     "都不足以單獨構成 MX-MAP；須有年報明確把產品對應到該營收線的揭露，"
     "否則該線維持待補證。**(4) 判斷順序**：先依正式定義判斷線內各產品本身是否為 collision（不得僅憑名稱或關鍵字重疊認定）→ 再判線內容證據是否達 (3) → 依各節點正式定義確認每項產品的歸屬 → 判型態（(7)）→ 逐節點裁定建列或不建列（(5)）→ 權重與狀態（(7)）。"
     "**(5) 建列**：證據支持在該線內、且依正式定義可歸屬某節點的產品，"
     "該節點**原則上應建立曝險列**，裁定者不得任意略過；"
     "對象限於證據支持且符合定義的產品與節點，不是所有可能相關的節點；"
     "不得把整條線的全部營收視為每個相關節點的營收。不建列只限下列原因，"
     "且須指出適用的規則、證據及受影響的產品與節點：**MX-SKIP-GATE**（節點有成員資格門檻或 mapping gate 而未通過，"
     "例：V7 受託製造證據門檻、R031 mapping gate）；"
     "**MX-SKIP-COLLISION**（該產品屬 collision，依 R031）；"
     "**MX-SKIP-UNDECIDED**（該產品的節點歸屬尚未確定，分兩種情形，"
     "不可互相代用：(a) **樹上沒有格**——有證據顯示現有各節點的正式定義都無法承接該產品，"
     "才依 ①-A (3)／R025 登記分類缺口；"
     "(b) **已有候選節點但尚未裁定**——產品可能屬於現有某節點，"
     "只是成員資格或節點邊界尚待裁定，記錄未裁事項、候選節點與待補證據，"
     "不因尚未裁定就推定缺節點或登記分類缺口。兩種都不得當作已證明不屬任何節點）；"
     "**MX-SKIP-MERGED**（同節點在同公司已有列，併入該列，保留來源追溯，"
     "不是排除該節點曝險）。證據不足以證明產品屬某節點，"
     "不等於已證明它不屬該節點：前者記證據或分類缺口，後者才是排除裁定。"
     "本項的「應建列」是規則層要求，不是個股裁定完成，也不是 DB 寫入授權；"
     "R027（未被曝險覆蓋的營收不等於公司沒有該業務）不得作為無證據補建曝險的理由。"
     "**(6) 其他判準不變**：exposure_type 依 R018、R022、③ 判定，"
     "不因屬混合線而一律 Direct；母子重複依 ③，不得以應建列為由重複計入。"
     "role 依 R026。依本項建立、承載已宣告混合線的 Indirect 列，"
     "weight_status 同樣依 (7) 記值（TOO_COARSE 或 GRANULARITY_INSUFFICIENT），"
     "不記 NOT_APPLICABLE。"
     "(7) (iii) 末句「Indirect 列依 R020③ 本來就不計重，"
     "不適用 (iii) 的記值」只處理未證實線涉及 Indirect 列的情形，"
     "不得用來改變承載混合線的 Indirect 列的記值。"
     "**(7) 型態、權重與狀態**：**MX-FULL（完整混合）**——線內產品的節點皆可具名、只是營收未拆分：所建各列 weight_pct＝NULL，"
     "weight_status＝TOO_COARSE。"
     "**MX-OPEN（含未定組成的混合）**——正式證據已證明同線含已知產品及節點未裁定的組成且無法可靠拆分：已知節點的列 weight_pct＝NULL，"
     "weight_status＝GRANULARITY_INSUFFICIENT，"
     "未定組成不建列，依 (5) MX-SKIP-UNDECIDED 的情形記分類缺口或未裁事項；"
     "不得因資料缺漏、產品資訊不清或尚未完成調查而判為 MX-OPEN。未定組成日後裁定，"
     "依 ①-S 收斂條件轉為 TOO_COARSE 或 WEIGHTED。"
     "同一營收線含 collision 產品時，collision 項不由本項建列，"
     "依 R031 與 ② 甲型處理（R031 未解前不新增、不更新該產品的曝險）；"
     "其餘依本項建出的列：若同線其他組成的節點皆可具名，依 MX-FULL 記值；"
     "若另有已正式揭露、節點未裁定的組成，依 MX-OPEN 記值。"
     "同一列同時承載 MX-FULL 線與 MX-OPEN 線（含依 MX-SKIP-MERGED 併入 MX-OPEN 線）時，"
     "記 GRANULARITY_INSUFFICIENT。"
     "同一節點的任一組成項（含另一條獨立營收線）無法可靠歸屬或計算時，"
     "依 ④ 該節點整列 weight_pct＝NULL，不得提供部分和；"
     "weight_item 須列出該節點全部已證實的組成來源。新起草時，"
     "組成項因揭露粒度無法歸屬者，該營收線依本項判斷（④★ 修訂），"
     "三種情形分開處理：(i) 經證據證明整條屬於該節點的營收線列入該節點的 weight_item；"
     "該節點的 weight_pct 只在 ④ 的記值條件成立、且沒有 (iii) 的未證實線涉及該節點時，"
     "才依 ④ 計算；(ii) 同一混合線已證實含節點未裁定的組成者，"
     "已知節點依 MX-OPEN 處理（weight_pct＝NULL、weight_status＝GRANULARITY_INSUFFICIENT），"
     "不得以該線的已知部分計數；(iii) 有線索（MX-CLUE）顯示可能含該節點產品、但達不到 (3) 證據門檻的營收線，"
     "不列入任何列的 weight_item，逐條揭露為未證實線——它不視為已歸屬該節點，"
     "也不因未歸屬就視為已證明不屬該節點；該節點因此無法確認全部組成都已計入，"
     "屬 ④ 所稱無法可靠歸屬的組成：整列 weight_pct＝NULL，"
     "weight_status 依 ④★ 記值（未證實線的候選節點中除該節點外另有可具名節點者記 TOO_COARSE，"
     "否則記 GRANULARITY_INSUFFICIENT；涉及多條未證實線時，"
     "任一條屬後者即記 GRANULARITY_INSUFFICIENT）。"
     "Indirect 列依 R020③ 本來就不計重，不適用 (iii) 的記值，"
     "weight_status 維持 Not_Applicable。"
     "weight_item 與 evidence 列出的已證實來源是追溯紀錄，"
     "不得加總成該節點的權重或下限。既有列不因本句回溯。"
     "**(8) 禁止**：把整條線的比重套到任一節點；依推測比例拆分；"
     "以已知部分加總作部分權重或下限（(7) (iii) 有未證實線涉及的節點亦同：不得以已證實的線加總成該節點權重）；"
     "從非產品軸的線推導權重；違反 ②-BU 從事業群／部門線推導權重；"
     "因已建列就把未定組成視為已歸屬該節點。"
     "**(9) 揭露軸**：依應用、客戶、地區、會計科目、交易性質或行業別切分的線不屬本項，"
     "依 ①-S（NOT_DISCLOSED）；"
     "其他正式證據足以證明產品歸屬時仍可依既有規則裁定曝險，但不得把該等百分比轉為節點權重。"
     "依事業群、事業處或部門切分的線屬業務軸，"
     "須達 (3) 證據門檻且確認內容跨不同節點時才適用本項，"
     "並受 ②-BU 限制（不得推導節點權重，"
     "可作 Core／strategic identity 質性證據）；"
     "本項定何時屬混合線與一般處理，②-BU 定業務群／部門線的具體限制，"
     "兩者不得各自形成不同觸發標準。同一張表混用不同揭露軸時，"
     "先處理表格有效性（無效依 DISCLOSURE_INVALID；"
     "擷取錯誤交 parser），不得只因合計不等於 100% 就判無效，"
     "也不得在有效性釐清前用該表推權重或轉為本項。"
     "**(10) 附隨服務例外（MX-INC）**：下列四項同時成立時，"
     "安裝、試車調機、保固、售後維修保養不因其存在而使該產品營收線成為混合線——服務對象是公司自售產品；"
     "種類限上列四種；營業比重表未單獨列示；無正式證據顯示為獨立銷售業務。"
     "未單獨列示不等於已證明附隨，須有證據確認服務附屬於公司自售產品。"
     "不適用：系統整合、統包或工程專案、軟體、他廠產品維修、代客營運或門店運維、有證據的獨立銷售服務；"
     "營業比重表已單列的服務線依實際交付內容歸屬，無相應節點者維持未分類。"
     "財報「隨時間認列／於某一時點認列」不得單獨證明有或沒有獨立服務業務。"
     "本例外是觸發條件的例外，不是把服務營收推定為產品營收，也不代表可推算服務金額或比例。"
     "**(11) 新建列的機械要求**：依本項建出的列，"
     "weight_item 填該營收線品名（逐字）；"
     "weight_status 依 (7) 填寫，"
     "不得留空——只填 weight_item 而 status 留空，"
     "db.fill_weight 會把整條線比重回填給該節點；"
     "evidence 須寫明型態（「依 R020 ②-MX，"
     "完整混合（MX-FULL）」或「依 R020 ②-MX，"
     "含未定組成的混合（MX-OPEN）」），此字樣是審計紀錄，不用來辨識本項的列。"
     "某列是否屬本項，以起草時的混合線宣告為準，"
     "不以 evidence 文字或 weight_status 推定。"
     "既有列不因本項回溯：weight_item 為空的 TOO_COARSE 列、依 ①-S 單獨記 GRANULARITY_INSUFFICIENT 而 weight_item 為空的列、以及恰一列計重其餘標 NOT_APPLICABLE 的共享線形狀，"
     "均不因本項判為違規。本項代號只用於起草輸出、送審表與裁定紀錄，不另設 DB 欄位。"
     "機制：db.check_industry 的 [mx_shared_line] 只檢查結構——同公司同一營收線出現在兩個以上節點時，"
     "只允許 (i) 各列 weight_pct 皆 NULL 且 weight_status 皆為 TOO_COARSE 或 GRANULARITY_INSUFFICIENT，"
     "或 (ii) 組內沒有這兩種狀態、恰一列計重、其餘列標 NOT_APPLICABLE；"
     "它不讀 evidence，也不判斷某列是否屬本項。"
     "db.fill_weight 回填前先執行該檢查，"
     "有任何報出即中止回填（fail-closed）；中止不修正上一次回填留下的比重。"
     "該檢查看不到：只建一列的混合線；未宣告而形狀碰巧為 (ii) 的混合線；"
     "同一條線在不同列寫法不一致；weight_item 為空、實際對應另一列計重線的列；"
     "也不確認 (i) 的列是正確的本項裁定。這些缺口由起草時的宣告一致性檢查與送審核對承擔，"
     "兩者只能降低、不能消除該風險，不得宣稱已由檢查封住。"
     "**(12) 節點條款**：節點定義不得自訂混合線的觸發、建列義務或權重；"
     "可保留本節點的範圍與排除、防止錯誤推導的禁止句（例：不得以客戶身分、廠別或名稱擇一全額歸屬）、單純路由句；"
     "混合營收線一律引用本項。③ **判準只有一個："
     "母公司這一筆曝險，會不會與子公司的獨立資料構成同一經濟活動的"
     "重複統計**——會，則母公司該列為 Indirect。子公司佔多少、是否已上市，都不是判準。"
     "★**條文優先於直覺分類**（2026-09-08 台灣精銳實例）：4583 台灣精銳的「客房及餐飲服務 18.78%」"
     "來自轉投資之台中日月千禧酒店，直覺會判 Indirect（「轉投資子公司的營收」），"
     "但本條問的是重複統計而不是持股結構——日月千禧酒店並非獨立建檔之上市櫃公司，"
     "市場層級沒有第二筆計入，重複不會發生，故該列為 **Direct**（Y1 飯店 Major 18.78）。"
     "若日後認定本條的本意是「所有合併子公司營收一律 Indirect」，"
     "**應修改本條文並跑全庫 migration**，不得在單一公司上逕行改判準。"
     "適用情形是母公司合併營收線包含已於同一 Industry node 獨立建檔之合併子公司，"
     "且該營收已由該子公司在市場層級獨立計入。"
     "★不可寫成「主要來自」——那會把門檻變成爭論點；也不可因為「子公司已上市」就整筆判 "
     "Indirect：若該營收線同時含母公司自身的其他業務，重複的只是其中一部分，"
     "此時整列 weight 本來就是 NULL（②），Indirect 與否仍回到上面那個判準。"
     "該 Indirect 列的 weight_pct 一律 NULL，比重僅保留於 evidence——年報確實揭露了這個數字，"
     "不寫明就會有人依 ① 把它填進去，重複計數就從這裡進來。"
     "例：茂達離散式功率元件 44.91% 來自合併的大中，大中已自列 A4-22。"
     "④ weight_pct 為節點合計值，定義為「**可可靠拆分且屬於該 node 的 leaf revenue lines 總和**」"
     "——只有 leaf 組成的 node total 才是正式 weight：若年報同時列出 A5 50%、A5-05 "
     "20%、A5-06 30%，正式 weight 是 A5-05 的 20% 與 A5-06 的 30%，A5 那個 "
     "50% 是 subtotal，"
     "不另建一列（與 R027 分母同一個原則）。weight_item 可保留組成業務線，以 \" + \" 列出"
     "（創意 A4-05＝委託設計 23.53%＋晶圓產品 75.38%＝98.91%）。"
     "若**該節點的**組成線中任一項無法可靠歸屬或計算，則該筆曝險 weight_pct=NULL，不得提供部分和"
     "（範圍是這一個節點，不是整家公司——同公司其他節點照常計算）；"
     "已知組成項仍可保留於 weight_item／evidence。"
     "★組成項因**揭露粒度**而無法歸屬、致本列 weight_pct=NULL 時，weight_status 依 ①-S 判準記值：無法歸屬的部分所涉另一節點可具名者記 TOO_COARSE，不可具名者記 GRANULARITY_INSUFFICIENT。"
     "本句只決定記哪一個值，不改變本項何時 MUST 為 NULL。新起草時，"
     "組成項因揭露粒度無法歸屬者，該營收線依 R020 ②-MX (7) 判斷建列與記值。"
     "可能涉及本節點、但達不到 ②-MX (3) 證據門檻的營收線，"
     "屬本項所稱無法可靠歸屬的組成：不列入本列 weight_item（它未被認定屬於本節點），"
     "依 ②-MX (7) (iii) 揭露為未證實線，本列 weight_pct=NULL，"
     "weight_status 依上句記值。Indirect 列依 R020③ 不計重，"
     "不適用本句。既有列不因本句回溯。"
     "★機制：db.fill_weight 對 weight_status 屬 `db.weight_status.AUTOFILL_BLOCKED`（目前為 Too_Coarse、Granularity_Insufficient）且具 weight_item 的列，不以 weight_item 自動解析回填，weight_pct 維持 NULL——否則保留下來的 item 會被解析回填，等於從後門推導出節點合計。這是跳過自動計算，不是禁止該列有比重：狀態經重新裁定解除後，照常依 ① 計算。"
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
     "錯誤訊息直接指名還有幾列要搬。"
     "★**產品名稱相同或高度相似，不得單獨作為 exposure 歸屬依據**："
     "同一個品項詞會出現在價值鏈的不同段，歸屬須依公司**實際生產、加工、交付或提供**"
     "該產品／服務的位置判定。2026-09-07 顯示分支逐家裁定即此形：一家的最大營收線寫"
     "「TFT 84.13%」而年報所營業務是「液晶顯示**觸控模組**設計、製造業務」；"
     "另一家寫「液晶顯示器**模組** 97.67%」而商品項目寫「液晶顯示器**面板**之設計、製造」"
     "——同一組詞在兩個方向上都指錯。"
     "★**下游應用名稱不覆蓋產品本體的 value-chain role**："
     "「顯示器沖壓零組件」的產品本體是前框、背蓋、燈罩與接地板，屬機構件而非顯示產品。"
     "同理適用於 LED、觸控、光學這類同時是技術詞與應用詞的品項。"
     "★**一句話版本，供其他分支直接引用**："
     "**同名產品不構成同一 Industry；應依其服務對象、實際用途及價值鏈位置判定，"
     "而非依產品名稱判定。**"
     "遇到「感測器」「模組」「薄膜」「晶圓」「鏡片」「光學元件」這類跨產業名稱時引用本條，"
     "不必每個分支重新發明判準。"
     "★**已完成的四路先例**（2026-09-08，AA 成像與精密光學建立時）："
     "同一個「薄膜濾光片」，服務光通訊模組者屬 D6-01、以晶圓製程沉積者屬 A3-05、"
     "服務顯示器背光者屬 E2-02、位於成像光路者屬 AA1；"
     "同一個「鏡片」，矯正人眼視力者屬 N3、成像光路者屬 AA1。"
     "**判準是它在誰的光路上，不是它叫什麼**——這條可泛化到光學膜、光學鍍膜、"
     "光學元件等所有同時是材料詞與用途詞的品項。"
     "★★**具獨立產品／設備 identity 者，終端市場是 exposure context 而非分類軸**"
     "（2026-09-08，由 G4 與 G5 兩支各兩個實例歸納）："
     "當一項產品本身已具備可獨立辨識的產品或設備 identity 時，"
     "**它賣給誰、最終加工出什麼，是 exposure 的脈絡，不是決定 Industry 歸屬的首要分類軸**。"
     "**例外**：該產品本身已是特定產業的專用設備，且樹上已存在該產業的專屬分支——"
     "此時依既有分支歸屬（半導體設備見 A2、PCB 設備見 B5、面板設備見 E7）。"
     "四個實例：4583 台灣精銳的減速機與 8083 瑞穎的軸承服務自動倉儲、車庫門、手推車等"
     "非工業製造的終端市場，仍屬 G4-01 傳動元件；4544 春日的成型機只服務扣件生產、"
     "4526 東台的專用機專為特定零件大量生產而設計，兩者仍屬 G5 工具機。"
     "★**與前一段的關係**：前段講同名產品要看價值鏈位置（名稱不足以定案），"
     "本段講的是相反方向的誤判——產品 identity 已經明確，卻被終端市場拉走。"
     "兩段共用同一個判準：**看產品在價值鏈上做什麼，不看它叫什麼、也不看它賣給誰**。"),

    # The draft names "A3 Memory" etc., which R005 forbids and the tree does
    # not contain. Left as drafted it is a copy-paste source of bad codes.
    ("R017", "HBM＝Theme",
     "HBM 為 Theme T07；若公司涉及 HBM，其 Industry Exposure 依實際價值鏈角色分別歸入"
     "對應節點，具體落點依該公司的實際價值鏈角色判定，本條不列舉節點代碼（原稿列舉的 A5-04 HBM Packagin"
     "g 與 A6-07 HBM Test 皆非現行節點——A6-07 已移出本樹、A5-04 從未存在，理由見 R005"
     "）。"
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
     "屬此情形的列數以當次查詢為準（`evidence_kind IN ('company_site','investor_conf')` "
     "且 known_date 等於該公司年報申報日者），不寫進正文（理由見 R027）；"
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
     "★**R023-R｜taxonomy 變動後必須重驗 Core**（2026-09-08 先例，1595 川寶）："
     "新增或拆分 leaf 之後，**受影響公司的既有 Core rationale 必須重新驗證**，"
     "不得因為 Core 歷史上就掛在那裡而保留。"
     "川寶原 Core 在 A2-03 薄膜沉積，理由是「本格是該部門年報具名的第一項」，"
     "而那第一項是「半導體沉積 PECVD 製程設備**再生**」——A2-15 設備再生建立後，"
     "理由所指的活動已經有自己的格，A2-03 只剩 PEALD 新機，Core 依本條移至 A2-15。"
     "★機制：taxonomy refinement 會把一個過粗的曝險（例如「半導體設備」）拆成"
     "新機製造／設備再生／零組件／工程服務，"
     "**Core evidence 必須重新投影到細分後的 leaf ontology**；"
     "沒有這一步就會累積一批「曾經合理、現在已不合理」的 stale Core。"
     "★連帶規則：「年報具名的第一項」這類**揭露順序**只能當支持證據，"
     "不得單獨作為裁定依據；要據以裁定，需有第二個獨立來源一致，"
     "且該項本身是可辨識的獨立經濟活動。"
     "★**相同品項字面之判定例外**：當同一產品名稱可對應到不同 value-chain node 時，"
     "應以公司**創造價值的位置、實際交付內容與業務本質**判定，"
     "不得以產品名稱、終端應用名稱或下游客戶名稱直接決定 Core 或 Industry。"
     "本條與 ①～⑤ 的分工是：本條決定**候選集合怎麼成立**，①～⑤ 決定候選之間誰勝出——"
     "候選集合若是用產品名稱字面湊出來的，優先序再嚴謹也裁不出正確結果。"
     "節點端的對應條文見 R016。"
     "★★**R023-R2｜資料可得性改變時，也 MUST 重跑 Core**（2026-09-10 增訂，4923 力士）：R023-R 說的是**樹**變動之後要重驗 Core。本條補上對稱的另一半：**證據**變得可讀之後也要。"
     "力士原本的 Core 在 A4-07 電源管理 IC，evidence 開頭寫「年報無比重表；類比電源管理IC 設計」。那句話本身是錯的——年報有比重表，只是 scrapers.annual_report_revenue 把年報代碼硬編成 F04／FE4，而該公司的年報是 F19，於是整家公司被記成「沒有申報年報」。選檔規則改為依 `{fiscal_year}_{stock_id}_` 前綴之後，比重表讀了出來：MOSFET 65.91、Wafer 24.21、ESD 7.18、Diode 1.41、Transistor 1.29——**這家公司賣的是功率分立元件，不是電源管理 IC**，Core 依本條移至 A4-22。"
     "★**這個先例的重點不是「新數字改變了 weight」，是「新數字換掉了節點」**：一個建立在『查不到資料』之上的分類，在資料查到之後被自己的資料推翻。所以 [weight_status_lifecycle] 亮燈時，正確的反應不是把 weight 補上就算完成，而是回到 R023 從頭問一次「這家公司的產業定位是什麼」。"
     "★機制：R020 的 NOT_EXTRACTED 轉出時（見 R020-NE）一律經過本條。"
     "★★**taxonomy 動作使候選集合改變時，MUST 重跑 Core 裁定**——觸發有二：(a) **新增 Industry Node**；(b) **既有 Node 的 Definition 擴充**（後者一樣會讓原本無處可掛的業務變成合法候選，2026-09-03 J4-01 擴充納入界面活性劑後，和桐的脂肪醇系產品即由「無節點」變成 Core 候選）。兩者皆：當新增 Industry Node 讓原 Core 出現"
     "新的高權重候選，不得因原 Core 已存在而直接沿用；流程是「新增節點 → 曝險重算 → 候選集合改變"
     "→ 依 ①～⑤ 重裁 → Core」，而不是「新增節點 → 保留原 Core」。"
     "否則分類系統會產生路徑依賴：新增的節點永遠無法挑戰既有 Core，先建檔的那一格就永久勝出。"
     "實例：3663 鑫科新增 L10 貴金屬（55.97%）後，候選從「A1-09 靶材 8.53% vs L9/L6」變成"
     "「A1-09 8.53% vs L10 55.97%」，必須重裁——重裁結果仍為 A1-09，但那是走完 ①～⑤ 的結論，"
     "不是沿用（理由記在該列 evidence）。"),

    ("R024", "上游核心能力與下游整合產品可並存，下游不得僅因營收較高自動奪走 Core",
     "公司同時從事上游核心能力與其下游整合產品時，兩者可各自建立曝險；"
     "不得因下游營收較高而自動把 Core 改為下游產業。"
     "★**本條不限於 IC 設計與模組**（2026-09-08 標題泛化）："
     "IC 設計→模組、封裝能力→光收發模組、感測 IC→感測模組、晶片→板卡、"
     "處理器→系統，皆為同一形。原標題只寫 IC 設計，讓這條看起來像半導體分支的特例；"
     "實際判準一直是**兩條線的關係**，見下。"
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
     "★★★**R025-GATE：母體先於重疊**（2026-09-11，由 A5 與 J8 兩案反向驗證）。"
     "**任何 sibling taxonomy 裁決，MUST 先確認候選 leaf 存在可觀測的 "
     "revenue-bearing economic activity；若無可觀測母體，不得進入 R031 的 overlap 判定。**"
     "★判定方式不是讀定義，是**去看營收線**：取該節點現有成員在該年度的營收列，"
     "問「有沒有一條線是這個節點所描述的活動賺來的」。零條即為無母體。"
     "★**正例 A5（通過 gate）**：A5-02 先進封裝有三家成員、各有營收線，"
     "R031 隨即發現「晶圓級封裝服務」同時落入 A5-02 與 A5-05，裁決軸、A5-02 降級。"
     "★**反例 J8（未通過 gate）**：J8 電子化學唯一成員 1722 台肥**零營收列**，"
     "membership 建立在產業概況章的通則與「提升至電子級產品」這句策略意圖上。"
     "R031 的判準要求一條營收線，這裡一條也沒有——**判準從未取得啟動條件**，"
     "本案是 R025 的母體問題不是 R031 的軸問題，J8 於同日移除。"
     "★★**這道 gate 防的是把 R031 寫成「兩個 Definition 沒有 overlap → 可以共存」**。"
     "那句話不完整：沒有母體的節點永遠測不出 overlap，於是永遠看起來安全。"
     "正確順序是**母體 → overlap → economic identity／attribute**，三層缺一不可。"
     "★看到「電子級」「車用」「AI」「HPC」「綠能」「高階」這類詞時尤其適用："
     "第一個問題永遠是「這是不是公司目前可觀測的 revenue-bearing 活動」，"
     "不是「這兩格會不會重疊」。"
     "若公司主要業務具有明確、成熟且可獨立辨識的產業產品，而現有 taxonomy 無適當節點，"
     "不得僅為維持樹狀結構而強制歸入不相符的節點；應新增 Industry Node，並保留原有上層分類。"
     "節點代碼穩定即可安全新增：曝險以 node_code 為 FK，新增不影響既有成員。"
     "★**新 L1 分支的成立門檻**（2026-09-08 先例，AA 成像與精密光學）："
     "一組**高度一致、且已逐家對全樹既有節點排除過**的 population 即足以證明 parent 成立；"
     "**不要求每個 L2 role 在建 parent 時都已有足夠人口**。"
     "否則會因為「材料只有 1 家、模組只有 1 家」而錯過一個 parent——AA 成立當時（2026-09-07）該 parent 的可觀測母體是 9 家，分散在三個角色上。"
     "反過來，parent 成立**不使其 child 自動成立**：AA 建立時只批准 1 個 leaf，"
     "材料、模組、系統三個角色各只有一例，列為 candidate 記於 db/TAXONOMY_GAPS.md。"
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
     "那就不是無歧義涵蓋，該新增節點。"
     "★★★**Node Definition 的建立與修訂必須以 taxonomy-level 的產品／服務邊界為依據，"
     "不得以單一公司或單一個案作為 Definition 的必要條件。Definition 完成後，"
     "始得進行 company-level exposure mapping；若既有多個 Node Definition 產生重疊，"
     "應先解決 Node 間邊界，再進行公司 mapping。**"
     "這比「不要為了收某家公司回頭補定義」更完整，因為個案污染只是其中一種失效："
     "另一種是**改完 A 才發現 A 與 B 重疊**，公司到底掛哪裡變成沒有答案。"
     "實例：2026-09-03 重寫 V1 定義時草案寫入「被動元件」，"
     "而 V2 被動元件通路是一個已有成員的既有節點——3090 日電貿的 Core 正是"
     "因為被動元件 81.35% 大於半導體 7.87% 才判在 V2；"
     "若 V1 也宣稱被動元件，那家公司的 Core 立刻無解。"
     "邊界要先解，順序不能倒過來。"
     "★★★**Residual node MUST NOT 由排除法建立**（2026-09-11 升格為全樹規則，原為 G6 "
     "的個別裁定）。觸發條件：有人主張「這家公司不屬於現有任何節點，所以應該有一個"
     "『其他 X』節點收它」。動作：**拒絕**。「不屬於 A、B、C」不是一個 economic "
     "identity，它是一句關於**樹**的陳述而不是關於**公司**的陳述；由它建立的節點，"
     "其成員之間沒有任何共同點，因此永遠寫不出 Definition，也永遠無法驗證互斥性——"
     "它從誕生那天起就同時違反 [definition_completeness] 與 [definition_boundary]。"
     "★成立節點的**唯一**途徑是正向的：指出一個可描述、可重複判斷、且有可觀測母體的 "
     "economic identity，再逐一對既有候選父節點做「無歧義涵蓋」檢查。"
     "★**先例**：G6 不得因為「不是 G5」而成立；V6「其他電子零組件通路」即為此型，"
     "應降為 unresolved 而非正常 mapping 目的地。"
     "★**辨識法**：把候選成員列出來問「這些公司彼此有什麼共同點」——"
     "若唯一的共同點是「都不屬於別格」，那它就是 residual。"),

    # role had no threshold at all: Major ran down to 6.90% while Minor ran up
    # to 19.26%, so the same share was labelled both ways.
    ("R026", "Major / Minor 的判準：數值為預設，偏離須具名",
     "Core 依 R019／R023 判定（產業定位，不由數字決定）；**非 Core 的 role 走數值預設**："
     "weight_pct >= 15% 為 Major、< 15% 為 Minor。"
     "★★**15% 是 Exposure Role 的操作門檻，不是產業重要性**（2026-09-08 增訂）："
     "Major 的意思是**這門業務對這家公司相對重要**，"
     "不是「這家公司在該產業裡是重要公司」，也不是該產業本身重要或該業務具策略地位。"
     "兩者是完全不同的量，而且方向常常相反——一家小公司可以在某節點是 Major 而在市場上無足輕重，"
     "台積電對某個小節點的曝險也可以是 Minor。此門檻與 R020 的 High／Medium／Low "
     "曝險級距亦不同源，不得互相換算。"
     "★★**Emerging 自 2026-09-08 起不再是本條的 role 值**：它是業務狀態不是排名，"
     "已由 migration 103 的 `cie_role_is_a_ranking` CHECK 禁止寫入 role（見 R019）。"
     "下文關於 Emerging 的兩條件與判準**全部保留**——它們定義的是那個業務狀態本身，"
     "在 business_status 欄建立後原樣適用；改變的只是它不再擠在 role 這一欄裡。"
     "★weight_pct 為 NULL 且非 Core 者，**依序判定**：先問「有沒有 evidence 證明它是附屬業務」"
     "——有 → Minor；沒有 → Major。**Major 是 default 不是判定結果**，"
     "寫成「預設 Major，再看看要不要改 Minor」會讓人以為 Major 是已經判過的結論。"
     "★「附屬業務」須有 evidence 支持，**不得僅因 weight 為 NULL 或營收未揭露而認定**，"
     "否則這個詞會變成下一個不可稽核的判斷。"
     "★★**「附屬業務」之定義**（2026-09-07 增訂）：係指 evidence 能證明該業務"
     "**相對於公司主要經營活動僅具附帶、配套、輔助或非主要之經濟／策略地位**；"
     "**不得僅以 weight=NULL、未揭露營收、營收占比不明或「非主要產品」字樣推定。**"
     "沒有這個定義，Minor 會退化成「NULL 的 fallback」——"
     "合格的寫法長這樣：5351 鈺創「系統晶片為記憶體以外之**次要產品線**」、"
     "3581 博磊「年報自述主力產品為測試產品、切割機及植球機，**清洗機不在其列**」、"
     "6129 普誠「年報自述主要營業項目為 TFT／馬達／LED 三種驅動 IC，**MCU 僅商品表一列**」，"
     "三者都先講出公司的主業是什麼，再講這條線相對於它的位置。"
     "不合格的寫法是 1722 台肥 J8 的「年報無營業比重表故不計比重」——"
     "那句解釋的是 weight 為什麼 NULL，不是 role 為什麼是 Minor。"
     "★這個預設不是「不知道就算重要」：能被建成一列代表該業務已被確認存在（evidence 必填），"
     "預設 Major 的意思是「無法量化、已確認存在、且沒有足夠理由認定它是附屬」。"
     "Emerging 須**同時**滿足兩件事：(一) 該業務**尚無可辨識營收**（weight 必為 NULL）；"
     "(二) 已有**明確布局 evidence**（擴產、合資、取得認證、法說會列為新事業等）。"
     "★只要已有可辨識營收，即應依 Direct/Indirect 與 weight 規則判定，**不得標 Emerging**——"
     "營收 1% 但大規模投入的公司是 Minor 不是 Emerging；Emerging 不是「比重很小」的同義詞，"
     "也不是「很看好」的同義詞。"
     "★數值預設**不適用**於兩種列，否則會互相吃掉：(一) **業務狀態為 Emerging 的列**——它的 weight 本來"
     "就必為 NULL，套用「NULL 預設 Major」會讓 Emerging 永遠不存在；"
     "(二) exposure_type='Indirect'——依 R020③ 其 weight 一律 NULL，套用預設會把「代理的線」"
     "或「子公司已計過的線」標成公司的重要業務。★★Emerging 與 Core／Major／Minor **不是同一種東西**：前者是**業務狀態**（尚無可辨識營收），後者是**推導出來的重要性排名**。兩者共用 role 欄位是既有 schema 的權宜，不是設計主張——R019 的候選資格只能引用前者，引用後者會讓 Core 自我增強。★**(一) 目前沒有任何列適用**：role='Emerging' 已由 `cie_role_is_a_ranking` CHECK 禁止，而 business_status 欄尚未存在；該欄啟用後才會出現實例，**屆時 MUST 寫在該欄、不得寫回 role**。★這兩種列既然沒有 weight，role 依 "
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
     "★★**Enforcement｜role 判定的三條路徑**（2026-09-07 增訂）。"
     "role 不是一種判定而是三種，各有各的依據與舉證責任："
     "**①Direct 且 weight 有值** → 純數值：>=15% Major、<15% Minor；毋須理由。"
     "**②exposure_type='Indirect' 或 business_status='Emerging'** → 數值預設不適用（現行 schema 尚無 business_status、role='Emerging' 已由 CHECK 禁止，故目前只有 Indirect 分支實際適用；Emerging 分支待該欄啟用後生效，見 R019）；"
     "若 role IN ('Major','Minor')，該列 evidence **必須明確記載 role 判定理由**，"
     "說明其經濟重要性或策略重要性的依據。"
     "**相關數字（包括子公司或代理業務之營收占比）得作為佐證，"
     "但不得以數值門檻或單一占比數字直接推出 Major／Minor。**"
     "★★禁止事項是「**不得單獨**」而非「不得引用」（2026-09-07 裁定）——"
     "後者會把本條自己的合格範例一起推翻，且無必要。"
     "不合格：「子公司虹冠電該產品占 8.05%，故 Minor」——真正做工的仍是一把數字尺。"
     "合格：「公司主要業務為功率分離式元件；虹冠電之功率積體電路業務相對於母公司主要經營活動"
     "屬另一條附帶產品線，對母公司整體經濟重要性較低，故列 Minor。"
     "子公司該業務營收占比 8.05% 僅作佐證」——真正做工的是「相對於主業屬附帶產品線」。"
     "判準是：把數字整句刪掉之後，理由還站得住嗎？站得住＝數字是佐證，站不住＝數字是那把尺。"
     "**③Direct 且 weight 為 NULL** → **不對稱**：Major 是本分支的 default，毋須交代"
     "「為什麼不是 Minor」；**標 Minor 則是一項關於公司的主張，必須有附屬業務之 evidence**。"
     "這個不對稱是刻意的——若兩邊舉證責任相同，無法量化的線會安靜地全部流進 Minor。"
     "★②與③的舉證內容不同，不可互相頂替：②問的是「這條**別人的**線對母公司重不重要」，"
     "③問的是「這條**公司自己做的**線相對於主業是不是附帶」。"
     "★★一筆 Indirect 其實承擔**兩個各自獨立的判斷**，同一個數字可以在兩處出現，"
     "但它在兩處扮演的證據角色不同，這不是衝突："
     "**(A) 為什麼是 Indirect** → 回答 R020③「為什麼這筆不能算成母公司的 Direct weight」；"
     "**(B) 為什麼 Indirect 之後 role 是 Major／Minor** → 回答本條。"
     "2481 強茂就是只寫了 (A) 沒寫 (B) 的例子：evidence 全文在說明「8.05% 來自子公司虹冠電、"
     "重複計數故不計比重」，那是 (A)；至於「這條業務在強茂整體經濟／策略版圖中的相對位置」，"
     "一個字都沒有。**「子公司業務占比」≠「母公司 exposure role」**——8.05% 能告訴我們"
     "虹冠電這條業務有一個可觀察的規模，不能直接推出它對強茂是 Minor。"
     "**Indirect rationale 與 role rationale 是兩件事，應分別寫出。**"
     "★★★三種數字必須分清，最不能發生的是把②或③當成①："
     "**①Role threshold number**——Direct 且有 weight，15% 門檻，數字**就是**判定；"
     "**②Role supporting number**——Indirect 或 weight 為 NULL，數字**只是**重要性的佐證；"
     "**③Weight-exclusion number**——用來說明該列為什麼不計 weight（R020②③），"
     "與 role 判定無關。2481 的 8.05%、3707 漢磊的 61.52%、2344 華邦電的 34% 都可以留，"
     "但三者都必須再有一句真正回答「所以，為什麼這對母公司而言是 Major／Minor？」。"
     "★★**Evidence-quality gate｜刪數字測試**（2026-09-07 增訂，人工 QA rubric）："
     "對任何非數值型 role rationale（即②③兩條路徑），"
     "**移除其中的具體數值後，若剩餘文字無法獨立支持 Major／Minor，該 rationale 不合格。**"
     "★測試的對象是**具體數值**，不是**量化資訊**——"
     "「纖維部為七部門中倒數第二小者」「合併營收占比最高的一條線」「三大產品線並列而無主從」"
     "這類**排序與比較**陳述可以保留且合格，因為它們回答的是"
     "「相對於主要經營活動的經濟位置」，只要背後的排序有 evidence 支持即可；"
     "不合格的是刪掉數值後只剩「因為占 8.05%」這種除了數字什麼都沒有的句子。"
     "換句話說，測的是**經濟／策略邏輯是否獨立存在**，不是「能不能完全不用量化資訊」。"
     "★★**合格 rationale 的形制**（2026-09-07 起統一）："
     "**(A)** 分類／曝險證據段——為什麼掛這一格、為什麼不計 weight（R020②③）——維持原樣；"
     "**(B)** 另起 `role 理由：` 一段，依序寫 **主業定位 → 該 exposure 相對於主業的位置 → "
     "Major／Minor 結論 → 「⋯僅作佐證」**。"
     "末句的固定收尾是給 reviewer 的訊號：這個數字不是在偷套 15% 門檻。"
     "★標準正例＝**2481 強茂 A4-07**：它示範了 Indirect 的子公司數字可以作證，"
     "但真正的 role 判定必須站在母公司視角完成——"
     "刪掉 8.05% 之後「強茂主業為功率分離式元件，虹冠電的功率 IC 屬另一條附帶產品線，"
     "非母公司自行經營亦非其能力投入所在」仍然獨立成立。"
     "★標準反例＝**1722 台肥 J8**：其 evidence 從未建立「電子級化學品相對於台肥主業屬附屬」"
     "這個環節，故 R026③ 的判定鏈根本沒有成立——"
     "處置不是把 Minor 的理由寫好一點，而是**Minor 的必要條件不存在，回到 default Major**。"
     "修正時加更正註記而非補寫 `role 理由：`，"
     "以免歷史資料看起來像是「曾經有過一個合理的 Minor 判定」。"
     "★為什麼要增訂：規則早有 normative requirement，資料驗證卻沒有對應的 invariant。"
     "2026-09-07 盤點時 12 列 Indirect 有 9 列沒有 role 理由，而 [role] 檢查報 0——"
     "因為它只驗 Direct 且有 weight 的列。2493 揚博是最清楚的證據："
     "同一家公司、同一張代理供應表、同樣沒有 weight 的三列，"
     "A2-13 標 Major、B6-02 標 Major、A1-05 標 Minor，沒有任何一列說明差別從何而來。"
     "★對稱關係：Core 不准憑品味，所以 [core_reason] 要求 evidence 可稽核；"
     "Indirect／Emerging 的 role 不准憑數字，所以本條要求 evidence 可稽核。"
     "★★檢查只驗到 Level 1（理由存不存在），驗不到 Level 2（理由合不合理）——"
     "「role 理由：因為很重要」會通過字串檢查而通不過閱讀，"
     "字串是地板不是天花板，實質仍屬 evidence review，與 [core_reason] 同一種分工。"
     "★db.check_industry 因此拆成三個檢查，與上述三條路徑一一對應（2026-09-07）："
     "**[role_numeric_consistency]**＝①，列出偏離數值預設的列（Direct 且有 weight，Major<15 或 Minor>=15）；"
     "**[role_indirect_rationale_missing]**＝②，列出 Indirect／Emerging 標 Major／Minor 卻沒寫 role 理由的列；"
     "**[role_direct_null_minor_unsupported]**＝③，列出 Direct 且 weight 為 NULL 標 Minor 卻沒有附屬業務依據的列。"
     "三者都不判對錯——有具名理由的偏離是合法的——但沒有它們，這一欄會無聲漂回訂定前的狀態。"
     "★三個檢查共用同一個 Level 1 標記 `role 理由：`（合格的 ③ 列已經這樣寫），"
     "但②與③要求的**內容**不同（見上）；標記是地板，內容仍須閱讀。"
     "**舊的單一 [role] 檢查報 0 時，只代表「沒有 Direct 且有數字的列違反門檻」，"
     "不代表 role 這一欄可稽核**；把它讀成後者是這次增訂的直接起因。"
     "★三條路徑各有多少列、其中多少列缺理由，一律以上述三個檢查的當次輸出為準，"
     "不寫進正文（理由見 R027）。"
     "★**本條增訂的起因（2026-09-07 的一次性盤點，非現況）**："
     "「Direct、weight 為 NULL、非 Core 而標 Minor」這一款從 2026-09-02 的 0 例增為 24 例"
     "（Direct 22＋Indirect 2，其中 16 列未寫理由），"
     "而當時唯一的 [role] 檢查只驗「Direct 且有 weight」的列，全程報 0——"
     "一款用法從未被行使變成被大量行使，中間沒有任何檢查看得見。"
     "記下這段是為了說明三個檢查為何要拆開，不是對現在的資料下斷言。"),

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
     "（「銷貨收入」在庫裡出現得相當普遍、比重也不低，排除的理由是**列名不指名產品**，"
     "不是該筆營收不算產品。）"
     "★已知限制：年報解析偶爾會把正文片段當成營收列（「◆ 搭配 21%」「單位:新台幣(仟元) 6%」），"
     "使該公司的分母灌水超過 100%；是否仍有這種公司、有幾家，以當次查詢為準（每家 `sum(weight_pct)`），不寫進正文。這種公司的 coverage 屬**解析異常下的非可靠值**，"
     "不應視為正常量測結果。★不可寫成「下限」——分母被撐大會低估，但漏抓營收線會高估，"
     "沒有證據顯示誤差只有單一方向。"
     "★db.check_industry 的 [coverage] 檢查列出覆蓋率 <70% 的公司（門檻可用 "
     "--coverage-threshold 調）；此為**提示不是阻擋**，載入器不因覆蓋率低而拒收——"
     "補不補是排程問題，但看不見才是資料問題。"
     "★**有比重表卻一格都沒指派**的公司也必須出現在這張表上（覆蓋率 0%），"
     "所以該檢查以「已分類公司」為左表 LEFT JOIN 權重合計，不可用 INNER JOIN——"
     "內接會把覆蓋率 0% 那批整批濾掉，正好漏掉本條要防的東西。"
     "覆蓋率 0% 那批分兩種，規則對它們的處置不同：年報只給一條粗線的（臻鼎-KY「各式印刷電路板 "
     "100%」）屬 R020② 的 `weight_status='Too_Coarse'`，是**已結案**；年報有可拆的比重表卻尚未去讀的"
     "屬**待補**，不得因為 weight 是 NULL 就當成「未揭露」。"
     "★**低覆蓋率有四種成因，不可混為一談**——[coverage] 的 coverage_status 欄回答"
     "「這個 58% 是什麼意思」：`parser_anomaly`（分母本身就錯，見上）／"
     "`coarse_filing`（該公司每一列的 weight_status 都是 Too_Coarse，沒有東西可指派，**已結案不是待補**）／"
     "`incomplete_mapping`（年報拆得比曝險細，**真的待補**）／`reliable`（達到門檻）。"
     "★四個值是同一套字彙，`reliable` 也是狀態不是「不在表上」——"
     "門檻決定誰被列出，不決定誰被標成什麼；要看全表含狀態就把門檻拉過 100"
     "（`--coverage-threshold 101`）。"
     "下游若要拿覆蓋率做自動選股或統計，MUST 先看 status——把 coarse_filing 當成資料缺漏"
     "會把一整批只能揭露一條粗線的公司誤判成建檔不全。"
     "★四種狀態的分布**以 db.check_industry [coverage] 的當次輸出為準**，不寫進規則正文——"
     "曝險一改數字就變，寫死的統計會讓規則長期處於「已知錯誤」狀態"
     "（本條初稿寫 165 家，隔一次修正就變成 164，正是這個問題）。"
     "★★**覆蓋率不能單獨當 KPI，要跟分子少掉那塊的 NULL 原因一起讀**：R020② 的 NULL 有兩種，"
     "taxonomy collision 型該修樹(R031)，Too_Coarse 型是來源揭露的限制、應永久保留。"
     "3131 弘塑、3581 博磊、3583 辛耘 的覆蓋率都極低，但那是年報只給一條橫跨前後段的設備營收線，"
     "不是我們漏建檔；硬拆成兩個看起來合理的數字會讓覆蓋率上升而資料品質下降。"),

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
     "★**這四欄的資料品質尚未開始建立**：換代帶過已實跑驗證，但驗到的是搬運 NULL。"
     "各欄的填充率以當次查詢為準，不寫進正文（理由見 R027）。"
     "★**首批完成度門檻**（不是每欄都要 100%，是給這份 Master DB 一個「可以開始被引用」"
     "的定義）：Core 100%／Industry Exposure 100%／known_date >90%／confidence >90%／"
     "supply_chain_position >50%／strategic_priority >30%／ecosystem 對照 >50%。"
     "**每項是否已達，一律當次量測**（Core 用 db.check_industry [core]，其餘用欄位的非空比例）"
     "——本條先前把「Core 100%（已達）」寫死在正文，而 [core] 後來報出數家沒有 Core 的公司，"
     "門檻清單自己就成了假陳述。known_date 另須注意其中一批的日期取錯來源（見 R021）。"
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
     "★T25 Theme 的成員數與此交叉形態是否已在庫裡出現，以當次查詢為準，不寫進本條正文"
     "（理由見 R027：寫死的統計會讓規則長期處於已知錯誤狀態）。"
     "已出現實例不使上述任何一條禁止失效。"),

    # A term being hot does not mean the tree has no home for it.
    ("R010", "熱門名詞不各自獨立成 Theme",
     "AI Agent / LLM / GenAI / ML 歸入 T35 AI Software，不各自開一個 Theme。"
     "★T35 是**跨技術／產品概念的 Theme 集合，不取代 U6 的 Industry 歸屬**："
     "若樹上已有適當節點（如 U6 AI Software），依 R005 仍應建立 Industry Exposure，"
     "Industry 與 Theme 並存，不是二選一——不得把 AI Software 這門生意整個塞進 Theme 了事。"
     "★T35 的成員數與 U6 的曝險數以當次查詢為準，不寫進本條正文（理由見 R027）。"
     "★本條的規範內容**不依賴這兩個數字**：無論 U6 已有成員或仍是空的，"
     "「熱門名詞歸入既有 Theme」與「Theme 不取代 Industry 歸屬」都照樣成立。"
     "先前這裡寫「此形態尚未在庫裡發生」，是把當時的計數當成論據——U6 後來有了成員，"
     "那句話就成了規則裡的假陳述。"),

    # These three describe machinery that exists and is empty. Left unannotated
    # they read as descriptions of the data.
    ("R007", "Supply Chain 獨立於 Industry",
     "一家公司可有多個供應鏈位置；供應鏈位置與產業節點是兩個維度，不互相決定。"
     "落地欄位是 tw.company_industry_exposure.supply_chain_position（FK 指 "
     "tw.supply_chain_position 字典），描述該**曝險**的位置而非公司（見 R028）。"
     "★**本條目前是設計不是描述**：字典與層級已建，但公司層那一欄的填充率要當次查"
     "（`SELECT count(*) FROM tw.company_industry_exposure WHERE supply_chain_position "
     "IS NOT NULL AND effective_to IS NULL`），數字不寫進正文（理由見 R027）。"
     "只要該欄仍是空的，「一家公司多個位置」就還沒有任何實例驗證過。"),

    ("R008", "Cycle 可多選",
     "Growth + Capex 等景氣屬性可並存，不是單選。"
     "★**多選機制尚未以關聯表落地**：tw.cycle_attributes 是純字典；公司層唯一載體 "
     "tw.company_profile.cycle_attributes 是逗號分隔的自由文字、無 FK，"
     "值與字典對不上（1605 寫 'Policy'，字典是 'Policy Driven 政策'）。"
     "有標註的家數與未標註的家數以當次查詢為準，不寫進正文（理由見 R027）。"),

    ("R009", "地區曝險與產業獨立",
     "Revenue 與 Production 的地區曝險分開標示，且與產業歸屬互不決定。"
     "★**本條目前是設計不是描述**：tw.region_exposure_rules 全是字典條文而非資料；"
     "公司層只有 tw.company_profile 的 china_revenue／us_revenue 兩欄，"
     "字典宣告的地區只實作 2 個；生產地是自由文字 main_production_site（如「台灣/中國/泰國」），"
     "未分級。**沒有 company_region_exposure 表**——這是結構事實，與筆數無關；"
     "各表的列數以當次查詢為準，不寫進正文（理由見 R027）。"),

    ("R013", "Group ≠ Theme",
     "Group 描述企業關係（母子/投資/關聯）；Theme 描述市場主題；分表存放。"
     "★group_type 僅限正式控制關係，**不得出現「正式集團 / 生態系」這種複合值**——"
     "生態系一律入 tw.ecosystem_master（R014）；GT04 台積電體系與 GT09 聯發科生態系"
     "原本就是這樣寫的，已改回單一值。"
     "★集團層（business_group／company_group，成員由 tw.company_affiliations 推導，"
     "derived_from 留有持股佐證）是本節唯一有實資料的部分；列數以當次查詢為準，"
     "不寫進正文（理由見 R027）。company_group 無 known_date 欄，R021 的三軸不適用於集團層。"),

    ("R014", "Group ≠ Ecosystem",
     "Group＝正式集團/控制關係；Ecosystem＝公司與特定客戶、供應商、平台、聯盟或技術體系"
     "形成的關聯網絡（不限客戶/供應商，聯盟型如 E06 MIH 亦屬之）。"
     "★ecosystem_master 已建 E01 NVIDIA／E02 AMD／E03 Apple／"
     "E04 Tesla・EV／E05 雲端自研 ASIC／E06 MIH。**本條是設計還是描述，取決於當次查詢**：只要 company_ecosystem_mapping 的成員"
     "仍集中在少數生態系、或證據欄位仍多為 pending／confidence D／known_date NULL"
     "（依 R021 對回測等同不存在），它就仍是設計。成員數與填充狀況以當次查詢為準，"
     "不寫進正文（理由見 R027）。"
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
     "[core_overlap] 檢查，**以該檢查當次輸出為準**。"
     "★**Core pending 的公司照樣可以建立 Industry Exposure、Theme、Supply Chain 與集團對照**"
     "——沒有 Core 只是主從未定，不是這家公司不能進資料庫；"
     "任何 `if no_core: reject_company()` 的實作都違反本條。"
     "★三種狀態各有多少家，一律以 db.check_industry 的 [core]（Core=0 者）、"
     "[core_pending]（待裁決的觸發）、[core_overlap] 當次輸出為準，不寫進正文"
     "（理由見 R027）。**Core=0 與 core pending 都是本條允許的合法狀態**，"
     "看到非零不代表資料壞了，而是有待裁決的工單。"),

    # The three axes were defined but never constrained against each other.
    ("R030", "時間一致性：三軸之間必須互相說得通",
     "**必須成立**：① `known_date <= data as-of`——資料截止日之後才知道的事，"
     "在那個時點不存在；② `effective_to IS NULL OR effective_from < effective_to`——**有效期間是半開區間 `[effective_from, effective_to)`，故 `from = to` 是零長度區間、不是合法的一天**；寫成 `<=` 會放行它，也與 R029 的 overlap 判定（端點相接不算重疊）不一致；"
     "③ `evidence_period_end <= known_date`（**僅當 evidence_period_end 有值時**）；"
     "★**不具期間語意的 evidence，`evidence_period_end` 應為 NULL，不得以推測日期填補**——"
     "公司網站產品頁寫「目前產品包含 Touch Controller」沒有報導期間，"
     "填一個看起來合理的日期只是把「不知道」偽裝成「知道」；"
     "本欄的 NULL 是有意義的值（R028 同精神）。"
     "★★**同一句話適用於 `known_date`**（2026-09-07 增訂）："
     "**`known_date` 是 provenance attribute，不是可計算的 fallback**——"
     "它必須來自 `evidence_kind` 所指名的那份文件："
     "`annual_report_table`／`annual_report_prose` → 該年報之申報日；"
     "`investor_conf` → 該場法說之日期；"
     "`company_site` → 該頁面之發布或擷取日；若無此日期則 **NULL**。"
     "**不得因為某一種來源沒有日期，就去借另一種來源的日期。**"
     "★這條增訂的直接起因是本規則只寫了一個軸就被照做了一半："
     "增訂當時（2026-09-07）24 列 company_site 的 `evidence_period_end` 全部正確地留 NULL（程式碼確實遵守了上一句），"
     "但同一批列的 `known_date` 被 db.seed_industry 結尾一段不看 evidence_kind 的回填"
     "塞進了年報申報日——2330 台積電 A3-01 因此宣稱「台積電是純晶圓代工廠」這件事"
     "是在其 FY2025 年報申報那天才變得可知。"
     "同一個推測日期，在 `evidence_period_end` 欄被規則擋下，在 `known_date` 欄卻長驅直入。"
     "★**方向性風險不對稱，但兩個方向都不可接受**：借來的年報日通常晚於官網可知時間，"
     "那是保守排除、不會 look-ahead，只損失樣本；但若官網內容是年報申報之後才更新的，"
     "該列就宣稱了比證據更早的可知時間，那是製造歷史知識。"
     "而我們沒有記錄官網是哪天讀的，**所以無從分辨自己落在哪一邊**——"
     "不可分辨本身就是不得填補的理由。"
     "★**`created_at` 不得充當 `known_date`**：它只能說明「最晚在這天系統裡已有這筆證據」，"
     "不能說明「這個事實何時公開可知」，語意不同。"
     "★**接受回測代價**：無法重建公開日期的列保留 NULL，在任何 as-of 查詢中被保守排除。"
     "少一點樣本可以接受，製造一個假的歷史可知日期不可以——"
     "與 R027「未覆蓋的營收只代表尚未建檔」同一個立場：可見的缺口優於無聲的污染。"
     "★db.check_industry 因此把舊的 [known] 拆成 "
     "**[known_missing]**（證據有日期可給卻沒給，屬 pipeline 缺漏，應為 0）與 "
     "**[known_unavailable]**（證據本身無可重建之公開日期，NULL 是正確狀態，"
     "列出是為了讓人知道有多少列被 as-of 查詢保守排除；此類列若**帶有**日期才是 finding）。"
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
     "★違反數**以 [temporal] 當次輸出為準**——列數會變，寫死在規則裡就會過期"
     "（R027 已經踩過這個坑）。"
     "★★**「含歷史層」這句話可能是對空集合成立的**：`effective_to IS NOT NULL` 的列數"
     "當次查了才知道，而只要它是 0，[temporal] 報零就只證明了現行層乾淨，"
     "沒有證明過任何一次換代寫得對。R021／R030 的三軸版本化在第一次真正換代之前，"
     "都還是未經檢驗的設計。"),

    ("R022", "通路/代理商的產品線不得僅據產品名稱認定製造歸屬",
     "若公司**某一業務線**僅具通路/代理性質，其產品中即使出現設備、材料、載板、IC 等製造品項，"
     "也不得僅因產品名稱將該業務認定為製造業務；該產品如需保留產業關聯，"
     "應建立為 Indirect Industry Exposure，**不得因此建立製造型 Direct 或 Core**。"
     "★同一公司的其他業務線各自獨立判定，通路性質不外溢：若年報明確揭露自有製造、產能、"
     "製造設備或製造營收，該業務線照樣建立 Direct Exposure，不因公司另有通路業務而降級。"
     "例：一家同時有通路業務與製造／設備業務的公司，其通路業務線建立 V1 Direct；"
     "若該通路所代理的產品需保留其對應製造節點的產業關聯，該製造節點以 Indirect 表示，"
     "比重則計在通路線上。港建 V1 Direct ＋ 設備 Indirect 即此形。"
     "★本條**不以任何個別公司的 role、比重或 exposure_type 現值作為規則內容**——"
     "那會把一次 DB snapshot 寫進規則；需要實例時以當次查詢為準（R027）。"
     "否則產業統計會把通路商錯算成製造商。"
     "判斷依據以年報為主：是否有佣金/代理性收入、產品線是否為具名製造品、正文有無自有產能描述。"
     "★官方分類僅作後驗參考不居優先（R002）——長華* 官方列電子通路業，但它的兩條線逐線看"
     "都是製造：IC 導線架 48.91%（A1-13）與封膠樹酯 29.29%（A1-12）皆為具名製造品且無佣金收入，"
     "故兩線皆 Direct；若以官方分類為首要依據會把整家公司誤判成通路。"
     "★**官方的上位大分類同樣不得直接複製成本樹的節點**（2026-09-08 G15 裁定）：主計總處《行業標準分類》29 中類分為 C291 金屬加工用機械、C292 其他專用機械、C29x **通用機械**，前兩者對應本樹 G5 與 G6，第三者一度被讀成本樹缺一個 L2 role。逐家讀年報後該傘自行散開——減速機與齒輪箱回 G4-01（節點早已存在且定義字面已列），半導體級真空泵浦回 A2-10／A2-14，水處理泵與風機的 identity 是環保設備，車用散熱風扇、汽車空調壓縮機、剎車泵回 H，冷氣壓縮機是家電零組件，裝置用微型泵閥早已被 G4-02 具名排除；剩下的空壓機、泵浦、液壓氣壓與馬達，**寫不出一句不靠排除法的共同 Definition**。★結論：官方分類能正確描述市場空間，可作 discovery 與 reference，**但一個官方大分類涵蓋許多真實公司，不代表那些公司需要共享一個 Master Industry leaf**。節點的成立條件仍是 economic role 加 sibling boundary，不是外部分類有這一格。"),

    # An end-market node and a product node can both look right for the same
    # revenue line, and R004 only allows one of them.
    ("R031", "應用型 Industry Node 不得僅因下游應用存在而自動建立 Exposure",
     "應用型 Industry Node（例如 H4-01 車用IC）**只有在公司自身提供具有明確車用產品、車規能力、"
     "專用設計或車用業務實質的產品/服務時**，才可建立 Industry Exposure。"
     "僅因一般 IC、電子零組件或模組「被使用於汽車」，不得轉列為車用 Industry。"
     "同一業務曝險仍受 R004 約束，**不得因應用領域不同而對同一營收線重複歸屬**——"
     "「A4-10 網通 IC 100% ＋ H4-01 車用IC 100%」是同一個經濟活動被算兩次。"
     "★**H4-01 現況＝Definition Approved / Mapping Gate Pending，未啟用**（2026-09-03 裁定）："
     "有 node_code、有產品定義，但不得建立成員。**整個 H 分支零成員是這道 gate 強制的結果，不是漂移出來的統計**；還有多少節點連定義都沒有，"
     "以 db.check_industry [definition_completeness] 當次輸出為準（"
     "不寫進正文，理由見 R027）。在定義補齊之前，關著沒有損失什麼。"
     "★★**未決的是架構不是個案**：Industry tree 究竟只走「產品／價值鏈」單軸，"
     "還是允許「產品 × 終端應用」雙軸同時作 Industry leaf？"
     "若為前者，車用應另建 Application / End-Market Exposure 而不是 Industry leaf；"
     "若為後者，R004 的「同一業務只能一個 Industry」必須先重新定義得極精確。"
     "**在這題裁定之前，任何人都不得把公司掛上 H 分支**，也不得為了收某一家公司而回頭補寫"
     "G2 工業電腦、H3-03 Telematics 等空定義節點的定義——"
     "那會讓 taxonomy 被個案反向塑形（2388 威盛的 Mobile360／Firewatch／4D+ 系統業務即卡在此處）。"
     "★啟用時的義務：H4-01 一旦開放，**MUST 對全表重新掃描**（MCU/PMIC/Driver IC/Sensor IC/"
     "Connectivity IC 各廠皆受影響），不得只把觸發這次討論的那一家掛上去；"
     "掃描時逐家問 R025 的「無歧義涵蓋」，而不是看到「車用」兩個字就掛。"
     "★本條的 Mapping Gate 只是**一般規則的一個特例**：任何 Node 的 Definition "
     "未定案、或與其他 Node 的 Definition 重疊未解時，都不得進行 company-level "
     "exposure mapping——通則寫在 R025 末段。"
     "★★**同一 parent 下的 sibling nodes，其產品／服務定義必須互斥**；若產生重疊，"
     "先修正 Node Definition，未解決前不得據此新增或更新 company-level Exposure。"
     "可以：V1 半導體通路／V2 被動元件通路。不可以：V1 半導體及被動元件通路／V2 被動元件通路"
     "——後者會讓 3090 日電貿那條 81.35% 的被動元件營收線同時是兩個節點的候選，Core 無解。"
     "★★**一組 sibling 必須沿同一個軸切**。不同維度的節點不納入該組的互斥判斷："
     "V1–V6 是**產品類別軸**（所經銷產品是什麼），V7 EMS 是**商業模式軸**（製造服務），"
     "兩者本來就不比較；為了讓「所有 V 都同層級」而去動 V7 是把軸混在一起。"
     "★★**Residual node（殘差節點）的兩條前提**："
     "(一) 其成立前提是**同層級的非 residual nodes 已具有明確、互斥、且足以覆蓋其預定範圍"
     "的 Definition**——V1–V5 沒定義完，「不落在任一者」這句話沒有意義；"
     "(二) **不得吸收已有其他 sibling node 可明確歸屬的產品**；若某產品理論上可歸入某個"
     "sibling，但目前該 sibling 的 Definition 不足以判定，應**先修訂該 Definition**，"
     "而不是直接放進 residual。少了 (二)，residual 會退化成「不知道就丟這裡」，"
     "最後必然爆量，而爆量的那一格等於沒有分類。"
     "★★**同一產品／業務線同時可合理落入兩個 sibling node，且衝突來自兩者定義用了不同的"
     "分類軸時，要先修 parent-level 的 taxonomy boundary，不得用 weight=NULL 或新增節點"
     "把重疊蓋掉。**這種 NULL 是 R031 違規的偵測器不是結論：R020② 只能忠實地報告"
     "「無法唯一歸屬」，它分不出原因是年報寫得粗、還是樹問了年報沒回答的維度。"
     "實例：A2-05 清洗設備是**機台類型**、A2-12 封裝設備是**製程階段**，一台用於封裝的濕製程機台兩邊都成立，"
     "於是 3131 弘塑 69.86%、3581 博磊 65.07%、3583 辛耘 41.90% 三條最大的設備營收線"
     "在兩格都拿不到權重。"
     "★★**A2 分支的既有 boundary**（2026-09-04 裁定，寫在 A2 母節點定義裡）：A2-01～A2-09 只涵蓋**前段晶圓製造**設備，"
     "A2-12／A2-13 涵蓋**後段封裝／測試**設備；**設備的技術型態本身不得跨越這條製程階段界線**"
     "——服務封裝的清洗機是 A2-12 不是 A2-05。這條界線不是新發明：樹上早就有 "
     "A2-08 晶圓檢測／量測（前段）與 A2-13 測試／檢測設備（後段），"
     "同一種機台功能被切成兩格，界線正是它服務哪一段製程。"
     "★★**Enforcement｜空節點不是排除的證據**（2026-09-07 增訂）："
     "**不得以 sibling node「目前無 Definition」或「目前無成員」作為產品不屬於該 node 之依據；"
     "分類判定必須建立於已定義且可比較之 node boundary。**"
     "延伸條款：**若 sibling set 中存在未定義 node，該 node 不得作為排除其他 node 的分類依據；"
     "若分類結論可能受該未定義 node 影響，應標記待 taxonomy boundary review，"
     "而非以空節點狀態完成分類。**"
     "★被禁止的推理形式是「A6-04 沒定義／沒成員 → 因此不是 A6-04」——"
     "那不是判斷，那是把 taxonomy 的待辦當成了證據。正確順序是"
     "**先各自定義 boundary → 確認互斥 → 再判斷產品屬於哪一格**。"
     "★實例（本條增訂的起因，2026-09-07 3581 博磊 A6-03）："
     "把「積體電路**預燒用**測試連接座」判入 A6-03 時，理由寫了「A6-04 Burn-in 目前無定義且無成員」。"
     "結論本身正確，因為年報「測試產品」對應的四個品項"
     "（半導體測試治具、半導體測試介面板、積體電路預燒用測試連接座、積體電路測試用測試連接座）"
     "**全部逐字落在 A6-03 已寫明的 Test Socket／Load Board／Test Interface 之內**"
     "——那是正面證據，不依賴 A6-04 是空的。違規的是多寫的那半句。"
     "★★**「待辦」與「分類依據」必須分開放**：A6-04 未定義屬 **taxonomy governance backlog**，"
     "應記為 follow-up（例：「Taxonomy follow-up：A6-04 尚未完成 Definition，"
     "待 sibling boundary 完成後進行反向 overlap review」），"
     "**不得寫進該列的分類 rationale 主判定句**。"
     "缺定義可以是 taxonomy backlog，但不能變成產品分類的反證。"
     "★★**Enforcement｜sibling 必須共享同一分類軸**（2026-09-07 增訂）："
     "**同一 parent 下的 sibling Industry nodes 必須共享同一分類軸；"
     "跨軸的產品屬性、終端應用與商業模式不得藉 sibling node 表達。**"
     "本條把既有的「一組 sibling 必須沿同一個軸切」從一句原則升成可執行的裁定依據："
     "當一組 sibling 混了多個軸，正確處置不是替缺定義的那一格補寫定義"
     "——那只會把錯誤的結構正式化——而是**先重整分類軸**。"
     "★實例（本條增訂的起因，2026-09-07 B2 PCB製造）：該組七格混了五個軸——"
     "B2-01 硬板／B2-02 軟板／B2-03 軟硬結合是**結構**軸；"
     "B2-04 高速高頻是**電性／材料**屬性；B2-05 HDI／SLP 是**製程／密度**屬性；"
     "B2-06 AI Server PCB 是**終端應用**；B2-07 製程代工是**商業模式**。"
     "後三者彼此並不互斥——同一片 PCB 可以同時是 HDI、高速高頻且用於 AI Server——"
     "**那是三個屬性不是三個類別**。"
     "更關鍵的是 B2-01 自己的定義把「HDI、Server/AI Server PCB、車用PCB」一併寫了進去，"
     "因此它同時是重疊的製造者與受害者：掛在它底下的那批成員，歸屬從來無法對照 "
     "B2-05／B2-06 驗證。"
     "裁定是把 B2 收斂為單一結構軸（B2-01／02／03），"
     "並把 B2-04／05／06 移出 Industry tree——屬性與應用應由 Theme／attribute 表達"
     "（既有 T42 High Frequency / Low Loss 與 T03 AI Server 已承載其語意；"
     "HDI／SLP 目前無對應 theme，記為缺口）。"
     "★**不得為了讓某一家公司有地方掛而讓跨軸節點留下**："
     "4909 新復興「高頻微波通訊基板 90.66%」在 2026-09-07 裁定當時是全庫唯一支撐 B2-04 的營收線，"
     "但一條乾淨證據證明的是「這種產品屬性存在」，不是「這一格應該是 sibling Industry node」；"
     "依 R031 taxonomy 要先定義再讓單一公司進去，故該公司留在 B2-01（高頻微波基板就是一片硬板），"
     "其高頻屬性另循 Theme 表達。"
     "★★**商業模式軸同理**：B2-07 PCB 製程代工已經有成員，"
     "但它與 B2-01～03 不同軸（受託加工是商業模式不是板材結構），"
     "**不因為現在有成員就自動成為自然 sibling**；它究竟是 Industry leaf 還是 business-model "
     "attribute，須隨 A／B 分支的整體設計一併裁定，此處先明示其不納入結構軸的互斥判斷"
     "（同 V1–V6 產品類別軸與 V7 EMS 商業模式軸的既有處理）。"
     "★db.check_industry 因此把舊的 [definition] 拆成兩個（2026-09-07）："
     "**[definition_completeness]** 問「這個節點自己有沒有定義」——"
     "**leaf 必須有產品 Definition；branch 在其子樹已進入實際使用（子節點已有定義、"
     "或子樹已有成員映射）時，必須有 scope-level Definition**，但 branch 的定義"
     "本來就可以比 leaf 抽象，不要求同樣細；"
     "**[definition_boundary]** 問 R031 真正要防的那件事——"
     "**同一 sibling set 只要已進入實際使用（該組合計有成員）且 sibling 數 >= 2，"
     "其中任何一個 sibling 缺定義，整組的互斥性就無法驗證**，"
     "該 sibling 即為 finding，**即使它自己沒有成員**。"
     "B2-05 是這一項的標準案例：它叫「HDI / SLP」却無定義，而 B2-01 的定義裡逐字寫著「HDI」"
     "且掛著整批成員——舊 [definition] 因為 B2-05 自己沒有成員而完全不報。"
     "★★★**兩軸共存何時停止安全——可機械執行的判準**（2026-09-11）。本條先前的處理是："
     "不同軸的 sibling 可以共存，只是不納入互斥判斷（V1–V6 產品類別軸 vs V7 EMS "
     "商業模式軸；B2-01～03 板材結構 vs B2-07 製程代工）。**那個寬容有一個前提，"
     "而前提從未被寫出來**：沒有任何一條營收線能同時滿足兩格。"
     "★**判準**：取該 sibling set 下任一公司的任一條營收線，問「這一條線是否同時"
     "落入兩個節點的 Definition」。**答案為否 → 兩軸共存無害**，互斥判斷各自在軸內"
     "進行即可（經銷與 EMS 是兩種不同活動，一條營收線只會是其中一種）。"
     "**答案為是 → 共存不再中性，MUST 裁決**，不得繼續以「不同軸所以不比較」擱置。"
     "★**實例（A5，2026-09-11）**：A5-02 先進封裝（**該裁決的結果即為 A5-02 移除**，見 "
     "A5 分支定義）是**技術軸**、A5-05 封裝測試代工"
     "(OSAT) 是**商業模式軸**，而「受託提供先進封裝」**是同一條營收線、同時滿足兩者的"
     "Definition**——3265 台星科、3374 精材、6239 力成的相似業務因此落在不同節點。"
     "此時 R019 的候選集合會同時收到兩格、Core 無解，而這不是證據不足造成的，"
     "是樹自己造成的。★**裁決方向不是替兩格補 precedence**：precedence 只是把選擇"
     "藏進規則裡，那條營收線仍然同時屬於兩格。正解是讓商業模式離開 Industry leaf "
     "這一層（OSAT／IDM／Foundry／EMS 屬 business model，不與封裝技術競爭同一層），"
     "或反之讓技術離開——但**兩者不得並存為 sibling**。★同一判準亦適用於 B2-07 "
     "與 A／B 分支的整體設計：該裁定在本條先前的文字中被懸置，現在有了解除懸置的條件。"),
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
