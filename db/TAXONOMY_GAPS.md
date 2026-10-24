# Taxonomy gap inventory

A gap is a population with evidence and no node. It is **not** a list of nodes
to create. Every entry here has already failed the test R025 asks for — that a
consistent, mutually exclusive, repeatable economic-activity boundary can be
shown — or has not been put to that test yet. The inventory exists so that
"no node" stops being mistaken for "should add a node", which is the error that
produced B2-04/05/06, A5-03/04, K3/K4 and X5–X10.

Two rules govern every row:

- **Counts are observed candidates, not populations.** They come from keyword
  search over parsed FY2025 revenue items, which cannot see the 202 companies
  whose revenue section never parsed, the 45 with no report, or the 69 whose
  largest line is worded 電子產品. See `project_revenue_discovery_gap` in memory.
- **A keyword hit is not a candidate.** Population discovery runs in three
  layers and only the third produces a number worth quoting:
  **P1 lexical hit** — the regex matched. **P2 semantic exclusion** — drop the
  matches the word itself misleads on; 「伺服」 matches 「伺服器」, and eleven
  server and chassis makers entered a G candidate set that way on 2026-09-08.
  **P3 economic candidate** — the revenue line or business activity actually
  belongs to the industry being tested, which usually needs the filing read.
  A count from P1 belongs in a work list, never in an existence argument.
- **A low count never closes a row.** Population size is evidence strength, not
  an ontology rule. What closes a row is a boundary that can be written and
  applied to the next company without reading this file.

Scale for context (2026-09-08): 471 classified companies; 1597 unclassified
active ordinary shares carry a FY2025 revenue line ≥20%.

## gap_type

Six rows does not mean six missing nodes. `gap_type` says what kind of thing
each row is, because they need different work and some need none:

| type | meaning |
|---|---|
| `population_pending` | a real population exists; nobody has adjudicated it company by company yet, so whether it is one branch, several, or additions to existing branches is unknown |
| `taxonomy_node_candidate` | adjudicated far enough that a node is plausible; still owes a parent scope and an exclusivity test before R025 is satisfied |
| `cross-role decomposition` | one label covering several economic activities; a single node here would repeat the E5 error. Must be split before any of it can be a candidate |
| `taxonomy_design_choice` | not a missing node at all. A boundary was considered and deliberately not drawn; the row exists so the decision stays visible and monitored |
| `evidence_insufficient` | the population cannot be measured with the evidence currently held, independent of any taxonomy question |

### What is *not* a gap

A gap row says **the tree has no cell for a real business**. Three other kinds of
absence look similar in a report and need entirely different work, so none of
them belongs here:

| looks like | actually | decided in |
|---|---|---|
| a company's business has no weight | we could not read the filing (`NOT_EXTRACTED`) | R020 ①-S, a parser/encoding work item |
| a company's business has no weight | the filing discloses nothing on a product axis (`NOT_DISCLOSED`) | R020 ①-S, terminal |
| an exposure has no date | `[known_missing]` / `[known_unavailable]` | a different axis entirely — about evidence dates, never about weights |
| an exposure has no weight | the same economic activity sits on two separately listed entities (**consolidation governance**) | not a weight question: the number is disclosed, legible and applicable. See below |

**Consolidation governance — 6138 茂達 / 6435 大中.** 茂達's annual report
discloses 離散式功率元件 44.91%, and that line is real, legible and squarely
inside A4-22. It is also the revenue of 大中, which 茂達 consolidates and which
is **itself listed** (6435, STOCK, active) and already carries A4-22 as Core
100%. Giving 茂達 a weight there would count one economic activity twice at the
market level; giving it none leaves an exposure with no weight for a reason none
of the four weight_status values expresses. Recorded here rather than solved,
because n=1 does not justify a fifth state, and because the next instance is
what would show whether the right answer is a status, a role, or a rule about
which entity carries a weight when both are listed. **Do not** read 茂達's
Not_Applicable as "a revenue share is the wrong measure" — it is the right
measure, held by the other entity.

The test is what would change the state. A gap closes when a **node** is built;
`NOT_EXTRACTED` closes when a **parser** is fixed; `NOT_DISCLOSED` closes only if
the **company** changes what it publishes. 4585 達明's 15.40% automation line is
the clean example of the first: the number is legible and undisputed, and it
still has nowhere to go.

---

## G1 · 消費品製造（成品）

| | |
|---|---|
| **gap_type** | `population_pending` |
| observed candidates | 37 unclassified companies with a ≥15% line naming 鞋／自行車／傢俱／球類／健身／寵物／隱形眼鏡／行李箱／文具／玩具 |
| evidence coverage | revenue items only; almost none of these have been read |
| current candidate nodes | none. A full-tree search of names and definitions for 皮／箱／包／成品／消費／民生／家用／生活／雜貨／其他製造／文具／玩具／運動／自行車／鞋 returned only J14 家用化學品 and W2-01 消費金融 |
| reason no node | never adjudicated. Opening a branch here would be reasoning from prior knowledge of the market rather than from evidence, which is what the X round ruled against |
| population status | includes 9950 萬國通 (行李箱 94.17%), held as taxonomy-gap pending when X9 was removed, and 6509 聚和's 辦公文具用品 22.00% and 1315 達新's 傢俱用品 3.28%, both unassigned since the same round |
| next audit | read a sample across the sub-groups before deciding whether this is one branch, several, or a set of additions to existing branches |

## G2 · 顯示整機／ODM（set assembly）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — evidence not yet sufficient to open one |
| observed candidates | 1 clean: 2489 瑞軒 (顯示器 52.30% + 數位電視機 12.77%). Three more matched and were each disqualified: stale revenue lines, a failed narrative fetch, a line with no supporting product description |
| evidence coverage | narratives read for all four |
| current candidate nodes | none. E1 requires an own panel process, E2 requires bonding modules, E7 is equipment |
| reason no node | R025: one company is not a boundary. The role is real — the panel maker in E1-01 also does 電視整機代工 inside its single 100% line — but its size cannot be measured while that line is undivided |
| population status | recorded in E1-01's definition |
| next audit | needs the coarse-filing bucket, not keyword search: 佳世達「電子產品 93%」and 中光電「影像產品 24.48%」are exactly the shape that would populate this and cannot be seen |

## G3 · 成像光學 — **CLOSED 2026-09-08**, into AA 成像與精密光學

| | |
|---|---|
| **gap_type** | `resolved` — a new L1 branch and one approved leaf |
| what happened | all 22 candidates' filings were read. Nine were one coherent activity with no home anywhere: they became **AA1 成像光學元件** under the new **AA 成像與精密光學** branch. The other thirteen resolved elsewhere, which is what made the nine a population rather than a residue |
| where the rest went | 光通訊薄膜濾光片 2 家 → **D6-01**（統新 99.98、東典 75.29，兩家的商品項目都明寫「光通訊」）· 半導體晶圓級光學鍍膜 → **A3-05**（澤米 54.05）· 隱形眼鏡 2 家 → **N3**，定義依 R025 放寬（精華 99.87、望隼 99.65）· 車用感測與車用光學射出件 → H 方向（6722 輝創、6867 坦德科技）· 金屬沖壓 → F4-01 方向（3685 元創精密）——**這三家只指出方向，尚未安置** · 儲存媒體 → 見 G8 |
| what the branch does not contain | 「光學」二字不是入場券。AA 的 scope 逐項排除光通訊 (D)、半導體晶圓級光學 (A3-05)、顯示專用光學 (E2-02)、視力矯正鏡片 (N3)。判準是**光路服務誰** |

## G3a · 光學材料（光學玻璃毛胚）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one company, deliberately not a node |
| observed candidates | 1: 3441 聯一光 光學玻璃毛胚 92.22% |
| evidence coverage | narrative read |
| current candidate nodes | AA exists as parent; no materials leaf under it |
| reason no node | R025: one company is not a boundary. Its own filing gives the strongest argument for the role — 「依 PIDA 分類，本公司所生產之玻璃毛胚係屬光電產業之**光學材料業**」— and also the reason the population is small: 「我國光學玻璃材料廠商目前僅有台資的聯一及日資的台灣小原二家公司」 |
| population status | unclassified, held here on purpose |
| next audit | look for optical materials beyond glass blanks before deciding — plastic optical resin, mould-glass preform, coating材料 |

## G3b · 相機模組（camera module assembly）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one company |
| observed candidates | 1: 3659 百辰 電腦相機模組 91.59% + 手持裝置相機模組 7.97% |
| evidence coverage | narrative read |
| current candidate nodes | none. AA1 is the lens body, not the module |
| reason no node | the role is real and is the same value-chain distinction E1/E2 draws for panels, but one company cannot establish it |
| population status | unclassified |
| next audit | camera-module assemblers are a known Taiwanese industry; the coarse-filing bucket is the likely place they are hiding |

## G3c · 光學引擎／光機系統

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one company |
| observed candidates | 1: 3504 揚明光 (取像光學 36.00, 光學元件 23.00, 車用 16, 3D打印機 12, 微投光機 10) |
| evidence coverage | narrative read |
| current candidate nodes | none |
| reason no node | its products are 光學引擎 (DLP／LCOS)、色輪、積分柱、投影鏡頭、稜鏡 — a system one layer above a lens. One company |
| population status | unclassified. Note its 光學元件 23.00% line alone would qualify for AA1, but its Core is the system business, so it is not split today |
| next audit | projector and optical-engine makers; overlaps with 微投影 and AR light engines |

## G4 · 通用 LED／光電發射元件

| | |
|---|---|
| **gap_type** | `cross-role decomposition` — at least three activities under one label |
| observed candidates | 13 unclassified with a ≥15% LED line, plus 5 found only in narratives (3591 艾笛森, 3717 聯嘉投控, 6168 宏齊, 6654 天正國際, 6278 台表科) |
| evidence coverage | narratives read for 8 of the packagers |
| current candidate nodes | none since E5 was removed. A3-06 化合物半導體製造 covers epitaxy and wafer process, not packaging |
| reason no node | ruled 2026-09-08: a general LED emitter is not display-dedicated. The node question was never asked, only the parent question |
| population status | three sub-groups that must not be merged by default: **packaging/emitter** (億光, 佰鴻, 榮創, 光鼎, 華興, 三一東林, 東捷 …), **invisible light IR/UV** (6559 研晶 90.72%), **lighting modules** (8111 立碁 55.55%, taxonomy-gap pending since E5-01 went) |
| next audit | each sub-group separately. Emitter and sensor may or may not be one boundary; that is a population question, not a naming one |

## G5 · 顯示專用材料 vs 光學零組件（internal, not a missing node）

| | |
|---|---|
| **gap_type** | `taxonomy_design_choice` — nothing to add; monitored only |
| observed candidates | 5 on E2-02: 偏光板, TAC 膜, 增光膜, 導光板與擴散板, 背光膜材 |
| evidence coverage | narratives read for all five |
| current candidate nodes | E2-02, which holds them all |
| reason no node | ruled twice, 2026-09-07 on heterogeneity and 2026-09-08 on layering. One supply chain is not one industry leaf, and these five span material → film → optical part → subsystem |
| population status | not a coverage gap; every member is classified |
| next audit | only if a cut appears that can be applied without reading the company. 「導光板是材料還是零組件」 is the question that has no answer yet |

## G6 · 驗證／檢測服務（安規、EMC）

| | |
|---|---|
| **gap_type** | `population_pending` |
| observed candidates | 3 unclassified |
| evidence coverage | revenue items only |
| current candidate nodes | none; the testing nodes under A6 are semiconductor test, not compliance certification |
| reason no node | never adjudicated |
| population status | first noticed as a coverage backlog item during the A6 round |
| next audit | small; may resolve into a services branch rather than a manufacturing one |

---

## Pending companies whose gap may now be closed

These are held as taxonomy-gap pending against nodes that no longer exist, and
the structure has changed since. **They need a ruling, not an edit.**

| company | held since | why it may now resolve |
|---|---|---|
| 6854 錼創科技-KY創 | 2026-09-07, when E3 Micro LED was removed | its filing opens 「主要從事微發光二極體(MicroLED)顯示器之研發、製造及銷售」 and its products are 6" RGB MicroLED COC plus the PixeLED display family. E1-01 顯示面板 was created afterwards and asks for an own panel process, which this looks like. Not placed, because E was closed to further edits on 2026-09-08 and because Micro LED display manufacturing has never been tested against E1-01's boundary in either direction |

## Not gaps

Recorded so they are not re-opened as gaps by someone reading a node's absence:

- **光通訊元件與設備** — D5-01 / D6-01 hold them; 15 observed companies sit
  outside the classified population for coverage reasons, not taxonomy ones
- **顯示器驅動 IC** — A4-12
- **觸控控制 IC** — A4-15
- **半導體光學晶圓製程** — A3-05, widened 2026-09-07 rather than adding a node
- **車燈模組** — H 汽車 holds the role; 3 observed companies unclassified
- **通用觸控輸入件**（薄膜觸控開關、鍵盤開關）— F5-01 板上零件
- **顯示器金屬結構件** — F4-01 機構; see
  `tests/fixtures/cases/3543_zhouqiao_application_vs_product.md`

## G8 · 記錄型光碟片（optical recording media）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one company, and it says so itself |
| observed candidates | 1: 2349 錸德, 光學產品 86.77% |
| evidence coverage | narrative read |
| current candidate nodes | F7-01 記憶體模組 covers the memory-card, flash-drive and SSD half of that line; nothing covers recordable optical discs |
| reason no node | the line is undivided (R020②) and the disc half has exactly one maker: 「錸德亦為當前日商以外，唯一具備從 CD、DVD、BD 與 AD 各式記錄型光碟量產能力之企業」. A node for a single producer is a company, not a category |
| population status | unclassified; not placed on F7-01 because the line cannot be split |
| next audit | none scheduled. If the disc business shrinks out of the filing the whole line becomes F7-01 and this row closes |

## Also unplaced after the G3 sweep

Not gaps of their own yet, recorded so they are not lost:

- **6979 勝釩 光學薄膜 93.63%** — a protective film applied to touch panels, cover
  glass and light guides during manufacture and shipping, and to semiconductor
  processes. A process consumable, not part of any optical path
- **3019 亞光 數位相機產品 19.67%** — finished cameras, from a company whose Core
  is on AA1. Same finished-goods shape as G2
- **6742 澤米 精密光學元件製造 43.73%** — OLPF for interchangeable-lens cameras,
  LOE elements and optical glass substrate processing in one undivided line, so
  it spans imaging optics, AR waveguides and material processing. **No longer
  unplaced:** on 2026-10-07 it was put on AA1 as Major with no weight
  (`Too_Coarse`) — the exposure is identifiable, the share is not. Its Core is
  on A3-05, ruled on 2026-09-08 but not written until 2026-10-07


## Unplaced lines of 8374 羅昇 — recorded 2026-10-07

None remain: all five named revenue lines have a row as of 2026-10-07. The
section is kept because two of the placements rest on a reading.

Placed: 自動化控制類 28.11% and 工業傳動類 15.57% on V4 (Core, 43.68%);
半導體設備材料銷售及服務類 11.96% on V1 (Major, `Too_Coarse`); 設備及耗材類
31.63% on G6-02 (Major, `Granularity_Insufficient`); 能源管理類產品 12.64% on
V9 (Major, `Granularity_Insufficient`).

設備及耗材類 is read as the strapping machines of 怡進工業. The filing never
says what is in the line: the name appears in the revenue table and nowhere
else. The reading rests on the business scope naming 「捆包機設備、耗材」, on
the line growing by 800,744 of the 857,991 the company grew, on the investee
note giving 怡進's business as 「捆包設備生產製造及銷售」 and on the 2026-05-29
investor deck; no document says in words that this line is 怡進. It was placed
on that reading by ruling on 2026-10-07, once G6 was sealed, and carries no
weight because the line also holds consumables that nothing describes.

能源管理類產品 holds three things in one figure: energy-saving engineering by
達昇能源 (the investee note gives it as 「能源技術服務」), own-brand UPS sold in
Europe by BlueWalker GmbH (「能源管理產品之買賣及服務」) and resold
energy-management products; the line's suppliers are Eaton, Voltronic Power,
CyberPower and In Energy. The bought-in UPS and power equipment put it on V9,
created 2026-10-07; the energy-service part has no node, so the row carries
no weight.

工業傳動類 was unplaced until V4's Definition was revised on 2026-10-07 to name
傳動與線性運動元件. Before that V4 named 氣動與液壓元件 and not mechanical
transmission parts, and under R031 a product the Definition cannot decide is
settled by revising the Definition, not by placing the company. The line had
been kept out of G4-01 on 2026-09-08 as distribution (R022).

Evaluated 2026-10-07. Packaging machinery had a cell in name only, the old
G6-01, and was given its own leaf, G6-02, when G6 was sealed into three
leaves that day. Transmission-parts distribution was a question
about V4's product list, not a missing sibling, and was settled by that
revision: of the three distributors read, 8374 羅昇 and 6215 和椿 sell
transmission and control parts in one line and 2359 所羅門 sells control
products only; none distributes mechanical transmission parts alone. 和椿 and
所羅門 are unclassified and wait on the V4 re-scan (core_reassessment id 168).
UPS was named
by three cells with no Definition between them; on 2026-10-07 I8-01 was
given a Definition and took 3043 科風, 6409 旭隼 and 3628 盈正, and UPS left
I7-01's list. Energy
services has no cell, and the four lines a name scan finds (3712, 6849, 2390,
2392) have not been read, so they are not yet one population. The scan
matches names and narrative wording only; a line with a generic name, as this
company's largest is, is not found by it.

## Unplaced lines of 2359 所羅門 — recorded 2026-10-07

Two of its five revenue lines, 29% together, have no row; a third, 其他電子業
2%, is not described. Recorded because the company has no Core: its largest
line and its research point at different businesses (R019 branch 3).

- **光電製造業 21%** — TN/STN/TFT-LCD parts: LCD modules made by 富相 and its
  plants, with agency of TFT-LCD cells and driver ICs alongside. Not read
  against the E branch.
- **電子通路業 8%** — agency of active, passive and protection components in
  one line, which spans V1 and V2.

Placed: 機電事業群（不斷電系統）40% on V9 (Major, `Too_Coarse`) — agency for
Cummins generators, Piller and Eaton UPS, Siemens busway and Jenbacher gas
engines, with own-brand generator sets assembled through 盈門能源 in the same
figure; V9 was created 2026-10-07. 智動化業群 29% on V4 (Major,
`Granularity_Insufficient`) — agency of Rockwell Automation and of robots,
with the vision unit's own products in the same figure.

## G9 · 液壓／氣壓動力元件（fluid power）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one clean company, excluded by name from two neighbouring cells |
| observed candidates | 1 clean: 4584 君帆（液壓元件 76.61%＋氣壓元件 7.77%＋系統整合 11.46%）。登記首項為 CA02050 閥類製造業，產品為圓形／柱型／薄型油缸與氣缸等執行元件，加壓力控制閥、方向控制閥、流量控制閥與電磁閥。P2 排除 2067 嘉鋼（油壓機械鑄件 15.86%，鑄件廠） |
| evidence coverage | narrative read |
| current candidate nodes | none. **G4-01 and G4-02 each exclude it by name**, which is the cleanest possible statement that a role exists with no cell: G4-01 because the fluid carries energy rather than the part transmitting motion, G4-02 because the fluid is a power medium and not a process fluid being regulated |
| reason no node | one company. The role is well bounded — the exclusions were written against this company as a live test case — but bounded is not populated |
| population status | unclassified |
| **see also** | **G15 (CLOSED)** — 2026-09-08 this row was tested as part of a 通用機械 family and the family was rejected. That is evidence this row stands **alone**, not that it is smaller than thought |
| next audit | pneumatic and hydraulic component makers are a known Taiwanese cluster; the sweep ran on revenue items only and 液壓／氣壓 is exactly the kind of term a coarse filing buries under 機械零件 |

## G10 · 博弈機台（gaming machine hardware）

| | |
|---|---|
| **gap_type** | `population_pending` — two independent companies, no parent identity decided |
| observed candidates | 2. **6441 廣錠**：營收線唯一大項為「工業電腦 98.66%」，但同一份年報的商品項目寫的是「1 工業電腦(**博弈產業**)之研發／生產／銷售／應用」，主要業務為「博弈機台設計開發、組裝銷售、相關博弈產品零組件研發、生產及銷售」，計畫開發為「高智能、高信賴性的博弈主機」與「防爆、防盜高標準的博弈機台」。**4916 事欣科**：「博奕及工業電腦 35%」（另 航太及國防工業 65%），商品項目「(1)博奕機台：博奕板卡、準系統、玩家追蹤系統及機台框體及其零組件 (2)工業電腦：各類工業電腦主機板卡及其零組件」 |
| evidence coverage | narrative read, both |
| current candidate nodes | none. Y5 娛樂／休閒 is a service branch (營運博弈場所／休閒服務), not equipment manufacture; A4-16 影像 IC is a chip cell |
| reason no node | **parent identity unknown.** The activity is confirmed and is not one company's anomaly, but which L1 it belongs under is not: 娛樂設備製造／商用機台／電子設備組裝／遊戲硬體 are all live readings and they lead to different trees. Per R025 a confirmed gap is not a confirmed node — same treatment as AA got before its L2 role axis was settled |
| population status | both unclassified. 事欣科 is mixed: its 65% 航太及國防 line is a separate unadjudicated question |
| next audit | before proposing a parent, sweep for 遊戲機台／娛樂機台／自動販賣／KIOSK／商用機台 — the suspicion is that gaming machines are one member of a wider **commercial machine assembly** role, and deciding on gaming alone would repeat the E5 error of naming a cell after the first population found |
| **why this row exists at all** | it was found inside a G2 audit, not a gaming audit. 廣錠's revenue line says 工業電腦 at 98.66% and would have entered G2 on lexical match alone. This is the strongest R016 case in the G branch: **營收品項名稱不構成產業歸屬**（see `db/seed_skeleton.py` R016）, and it is why G2's definition carries 博弈機台 as a named negative test |

## G11 · 跨製程工業自動化／整廠整合（cross-process automation & system integration）

| | |
|---|---|
| **gap_type** | `population_pending` — the activity is real; whether it forms an Industry territory is exactly what has not been shown |
| **origin** | opened 2026-09-08 by the ruling that **dissolved G1 工業自動化與產線整合**. This row is not a leftover from the deletion — it is the part of the deletion that was deliberately kept open |
| observed candidates | 2, and they are not alike. **4585 達明**：自動化整合方案 15.40%（另 機器手臂及相關零組件 83.87% 屬 G3-01）。**6125 廣運**：自動化設備 80.89%，所營業務為整廠自動化物流系統、自動輸送機、自動堆棧板機、自動倉儲設備、無人搬運車、機械手臂，另有停車設備、焚化爐、水處理、太陽能電池與液冷散熱 |
| evidence coverage | narrative read, both |
| current candidate nodes | none, by ruling. Process-specific automation already has homes and they cross-reference each other: A2-09（晶圓廠）、B5-03（PCB／載板）、E7-03（面板） |
| reason no node | **the word is not the territory.** 11 companies with an 自動化設備 revenue line ≥15% were swept; 7 were already dispersed to A2-09／A2-12／B5-02／B5-03／E7-02 by which process they serve. Of the remainder, 6654 天正國際 serves the passive-component process and 6234 高僑 serves TFT and solar — both process-specific too, just to processes with no equipment branch. That leaves 達明 and 廣運, and 廣運 is a conglomerate whose automation is intralogistics（touching R7 物流, G3 機器人, G5／G6 設備）. Two companies that do not resemble each other is not the AA-style population — a set that is internally consistent *and* already excluded from every existing home |
| **what would close it** | not a higher count. A boundary that separates cross-process integration from (a) the process-specific equipment branches, (b) A7-01 廠務系統／Facility Automation, (c) R7 物流, and (d) R022 代理與整合, written so the next company can be placed without reading this file. Until then a node here would be a residual — "not A2, not B5, not E7, not another process ⇒ here" — which is the catch-all shape the tree has been removing (G6「機械設備」, E5, B2-04/05/06) |
| population status | both unclassified, **by ruling, not by oversight**. A coverage audit must not read these two as newly missing classifications |
| next audit | 「系統整合」as a term is unusable for discovery here: of 25 companies with a ≥15% line matching it, 8 are already A7-01 廠務工程 and 14 are IT systems integrators belonging under U — **zero industrial-automation integrators**. Any future sweep must start from the delivered object, not the word. Note also that only 6 of those 25 have a parsed narrative, so the IT split is P2-by-line-wording and has not been read |

## G12 · 驅動元件（電動馬達與驅動器）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — a role G4's own axis names, with no cell |
| **origin** | opened 2026-09-08 when G1-01 was deleted. Its single member had to go somewhere and the ruling was explicit that these must be re-placed by economic role, **not auto-recycled** into whatever cell is nearest |
| observed candidates | 1 carried over: **2483 百容** 步進馬達 12.00%（Core 已在 A1-13 半導體 49%，另 F2-01 連接器、F5-01 繼電器 13%）。年報自述應用於辦公自動化、工廠自動化、醫療器械 — 非車用專屬，故 H5-01 車用動力馬達不收 |
| evidence coverage | narrative read |
| current candidate nodes | none. **G4 的軸文字明列「驅動」為本軸四個功能之一**（傳動與線性運動／驅動／流體控制／工具），而 G4-01 收傳動、G4-02 收流體控制、G4-03 收工具——驅動沒有格 |
| reason no node | one company, and that company is a component maker whose motor line is its fourth product, not its identity. Same shape as G9 流體動力: the axis names the role, the role has no cell, and one member does not settle it |
| population status | 2483 百容 unclassified on this line by ruling; its other three exposures are unaffected |
| **see also** | **G15 (CLOSED)** — the C29 三分 reading was tested and rejected 2026-09-08. ★Also note the same audit found **G4-01 is not empty for want of a role** but for want of mapping (five companies matching its declared list) — so "G4's axis has four functions and three cells" is weaker evidence than it looked. **Do not add G4-04 to tidy the axis** |
| next audit | sweep 馬達／motor／驅動器／drive on revenue items, splitting 車用（H5-01）from 工業 from 家電 |
| observed 2026-10-24 | **4576 大銀微系統** — 精密運動及控制元件 46.56%（「控制器、驅動器、線性馬達、直驅馬達、真空馬達、」「力矩馬達、伺服馬達、線性致動器、位置量測系統」, line break in the filing）: NO_NODE, no row. Linear actuators and controllers inside the line may have partial homes (G4-01, G2); not shown item by item |

## G13 · 跨製程真空鍍膜與電漿設備（cross-process coating & plasma equipment）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one company with a clear equipment identity and no branch whose scope reaches it |
| observed candidates | 1: **3580 友威科**（設備 74.67%）。商品項目為真空濺鍍設備（In-line 連續濺鍍機、爐式濺鍍／蒸鍍機 NCVM／SDC／EMI、太陽能設備、車鏡鍍膜、觸控面板鍍膜、OLED 鍍膜、IC 封裝鍍膜、汽車輪圈鍍膜、種子層鈦銅鍍膜）與電漿蝕刻設備（電漿表面處理機、細線路金屬化製程蝕刻、電漿孔底清潔） |
| evidence coverage | narrative read |
| current candidate nodes | none. **A2-03 薄膜沉積 (CVD／PVD／ALD) and A2-02 蝕刻設備 both carry a【前段晶圓製造】scope qualifier**, and this company's coating serves IC substrates, OLED, touch panels, solar and automotive parts — not front-end wafer fabrication. The qualifier is doing its job; the company is genuinely outside it |
| reason no node | one company, and the question it raises is not "does it exist" but "what is the family". Coating and plasma treatment equipment sold across six unrelated end-industries may be one equipment role or may be several |
| population status | unclassified |
| **why not G6** | ruled 2026-09-08: G6 may not be populated by exclusion. Putting this company in G6 alongside G14's inspection machines would recreate 「其他非工具機設備」, the residual G6 exists to avoid |
| next audit | sweep 鍍膜／濺鍍／蒸鍍／PVD／CVD／表面處理設備 on revenue items, and separately check whether the coating **service** providers (who coat parts rather than sell the machine) are a different role again — 友威科's own narrative opens with its coating *service* business before its equipment products |

## G14 · 跨產業光學檢測與篩選設備（cross-industry inspection & sorting equipment）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — same shape as G13, different role |
| observed candidates | 1: **2070 精湛**（光學影像篩選機 83.28%）。產品為分度盤影像篩選機、玻璃盤影像篩選機、連續式影像篩選機、渦電流篩選機、輸送帶影像篩選機、電子元件外觀瑕疵檢查機、藥錠異檢機、首件品檢報告機；自述應用領域為緊固件、粉末冶金、LED、被動元件、藥錠與膠囊 |
| evidence coverage | narrative read |
| current candidate nodes | none. The tree's inspection cells all carry a process qualifier — A2-08【前段晶圓製造】、A2-13【後段測試】、B5-02【PCB】、E7-02【顯示】 — and this company serves five industries none of which is any of them |
| reason no node | one company. Note it is **not** placed here for being unclassifiable: its identity (inspection and sorting machines) is perfectly clear. What is missing is a demonstrated population sharing that identity |
| population status | unclassified |
| **why not G6, and why not with G13** | ruled 2026-09-08. G13 makes coating machines and this makes inspection machines; they share only the property of not being machine tools, which is exactly the non-argument G6 must not be built on |
| next audit | sweep 檢測機／篩選機／檢查機／外觀檢查／分選 on revenue items **excluding** the four process-qualified branches, and test whether cross-industry inspection equipment is one role or splits by sensing principle (optical / eddy current / X-ray) |

## G15 · 通用機械（general-purpose machinery）— **CLOSED 2026-09-08**

| | |
|---|---|
| **gap_type** | `not a gap` — **closed by audit**. The umbrella was read as a possible missing L2 role under G; a company-by-company read dissolved it. Kept as a worked precedent, not as an open row |
| **the ruling** | 2026-09-08. 「通用機械」is an external statistical umbrella, **not an economic role a company can be in**. The audit question that settled it was set before the evidence: *is this a role a company can be in, or only a description of who its customers are?* — the latter |
| **the outside evidence** | 4561 健椿's FY2025 filing quotes 行政院主計總處《行業標準分類》29 中類 as a **three-way** split: **C291 金屬加工用機械設備製造業**、**C292 其他專用機械設備製造業**、**C29x 通用機械設備製造業**. The tree has G5 for the first and G6 for the second. **It has nothing for the third**, and G's declared L2 axis (零組件／整機／控制運算) has no position for a general-purpose whole machine either |
| the three sightings | **G9 液壓／氣壓動力元件** — 4584 君帆（液壓元件 76.61%＋氣壓元件 7.77%），excluded by name from both G4-01 and G4-02. **G12 驅動元件** — 2483 百容（步進馬達 12.00%），the role G4's own axis names and has no cell for. **new 2026-09-08** — **4552 力達-KY**（螺桿式空壓機 59.45%＋活塞式空壓機 28.45%＝87.9%，另電焊機與電動工具），自述應用橫跨「居家及個人裝修」到「大型工業、醫藥、化工、紡織及食品加工」 |
| evidence coverage | narrative read, all three |
| **what changed on 2026-09-08** | G12's `next audit` had guessed that G9 and G12 were "G4 的軸宣告四個功能而只有三格" seen twice. The official three-way split says that reading is too small: these may not be a missing *cell under G4* but a missing *L2 role under G*. Both readings are still open — that is what this row is for |
| reason no node | whether 流體動力元件、電動馬達與驅動器、空壓機 are one economic role or three is exactly what has not been shown. They share a negative (not a machine tool, not special-purpose process equipment) and a positive that may be too thin to build on (sold as general capability across many industries) |
| population status | all three unclassified |
| **how it dissolved** | every candidate resolved into a home that already existed or into a narrower role. ★「resolved」 names where each belongs, not that it was written: only 4583 台灣精銳 and 4571 鈞興-KY were placed (G4-01); 7530 鋒魁 is a tracked pending write (id 162, A2-10 `CHANGE_PROPOSED`); the others remain unclassified as of 2026-10-07. 4583 台灣精銳 減速機 74.08% and 4571 鈞興-KY 齒輪箱 34.94% → **G4-01, whose definition already lists 齒輪與齒輪箱、減速機 word for word**. 7530 鋒魁 真空泵浦 85.56% → A2-10（定義字面列「真空幫浦」）／A2-14（其自述為「半導體及光電產業等級真空泵浦**維修技術服務**」）. 6641 基士德-KY → its own words are 「主要從事**環保水處理相關設備**的研發、生產、銷售」; the pumps and blowers are that product line. 2235 謚源 風扇 70.24%＋鼓風機 19.89% → H（「各式**車款**之散熱風扇與鼓風機」）. 2249 湧盛 汽車空調壓縮機、4557 永新 剎車泵 → H. 4532 瑞智 壓縮機 → 家電零組件. 4568 科際精密 泵類 → 裝置用微型泵，**G4-02 的定義早已具名排除**. 3060 銘異 音圈馬達 → 相機模組致動器. 4506 崇友／4565 宏偉 電梯 → 建築設備 |
| **what was left, and why it still failed** | 空壓機（4552 力達 87.9%）、泵浦（6982 大井 95.81%）、液壓氣壓（4584 君帆）、馬達（4589 碩陽 94.89%、4590 富田 工業馬達 33.57%、2483 百容 12%）. No definition covers these without exclusion. 碩陽's motor modules serve 電動輪椅、代步車、高空作業車、農漁業、食品機械 and it also does OEM whole-machine assembly; 富田 self-describes as 「**電動載具動力系統**及工業馬達」 with US and Japanese carmakers as customers. Conveying a fluid (pumps, compressors) and using a fluid as a power medium (cylinders, G9) are already separated by G4-02's own definition |
| **the precedent** | recorded into **R002** in `db/seed_skeleton.py`: an official *upper-level* category may not be copied into the tree either. Official classification is discovery and reference; a node still needs an economic role and a sibling boundary. **A category covering many real companies does not mean those companies share a Master Industry leaf.** |
| what did NOT close | **G9** and **G12** stay open as separate candidates — this audit is positive evidence *against* merging them, not for it. And do not add G4-04 to make G4's declared axis look complete. 4552 力達 keeps its own row below |

## G16 · 工業空壓機與泵浦（air compressors & pumps）

| | |
|---|---|
| **gap_type** | `population_pending` — two companies, one shared property that may not be a role |
| **origin** | 2026-09-08, when G15 通用機械 closed. These two were the only candidates left standing that had no existing home, and the ruling was explicit that they must **not** be inherited as members of the rejected umbrella |
| observed candidates | 2. **4552 力達-KY**：螺桿式空壓機 59.45%＋活塞式空壓機 28.45%＝87.9%（另電焊機、電動工具）。自述應用橫跨「居家及個人裝修、小型規模公司及交通運輸」（活塞式）到「大型工業、醫藥、化工、紡織及食品加工」（螺桿式）。**6982 大井泵浦**：泵浦 95.81%，商品項目為「民生用泵浦、設備用泵浦、特機商用泵浦、泵浦相關維修服務」 |
| evidence coverage | narrative read, both |
| current candidate nodes | none. 真空幫浦 is A2-10's, but that cell is 半導體設備零組件; 水處理用泵與風機 belong to the water-treatment equipment identity (6641 基士德-KY); 車用與家電壓縮機 go to H and 家電 |
| reason no node | they share *conveying or compressing a fluid* and *selling into no particular industry*. The second half is a description of their customers, which G15 just established is not a role. Whether "fluid machinery" is one role or two (compressing gas vs conveying liquid) is untested, and with n=2 the question cannot be settled |
| population status | both unclassified |
| **not to be confused with G9** | G9 4584 君帆's cylinders and valves use fluid as a **power medium**; these two **move the fluid itself**. G4-02's definition already draws that line and it holds here |
| next audit | the discovery problem is that both companies' main line is named with an ordinary word (泵浦, 空壓機) that also names auto and appliance components. Sweep must run *after* excluding 車用／家電, as this one did, or the P1 layer returns mostly parts makers |

## G17 · 消費級 PC 主機板與顯示卡（consumer PC motherboards & graphics cards）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — a product category with no cell anywhere in the tree |
| **origin** | 2026-09-08 G2 mapping. 2399 映泰's two largest lines had nowhere to go, which is what forced the R019 ruling that a truncated candidate set may not hand Core to the only visible candidate |
| observed candidates | 1 so far: **2399 映泰** 電腦主機板 49.00% + 顯示卡 16.00% = 65%. Its third line, 工業電腦 11.00%, is mapped G2 Minor |
| evidence coverage | narrative read |
| current candidate nodes | none. **C10-01 個人電腦** is Desktop/Notebook/Mini PC/AIO - whole computers, not the board inside one; **C3-01 伺服器主機板** is explicitly the server side; **A4-02 GPU** is the chip, not the card |
| reason no node | one company, and the sweep that would find the rest has not been run. The category is real - Taiwan has several listed consumer motherboard and graphics-card brands - but n=1 does not establish a cell |
| population status | 2399 映泰 has no Core by ruling; only its G2 line is mapped |
| **do not** | build this as a 映泰-shaped cell. The question is whether 消費級 PC 主機板／顯示卡製造 is one role, two, or part of a wider PC-component role - and that needs the population first |
| next audit | sweep 主機板／顯示卡／繪圖卡 on revenue items, excluding 伺服器 (C3-01) and 工業 (G2). Expect the P1 layer to return board makers, ODM assemblers and distributors mixed together |

## G18 · 監控攝影機與錄影系統（video surveillance cameras & recorders）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — a product category with no cell anywhere in the tree |
| **origin** | 2026-09-08 G2 mapping, same shape as G17 and found in the same round |
| observed candidates | 1 so far: **5484 慧友** CCD 電子攝影機 31.06% + 影像處理器 27.43% + 網路音視訊傳輸系統 3.04% = 61.53%. Its 工業電腦 26.16% is mapped G2 Major. Product list: 嵌入式板卡及系統／車載錄影主機／CCTV 監控產品／工業電腦 |
| evidence coverage | narrative read. Its 產業概況 chapter is entirely about the video surveillance industry, so the filing itself says this is the company's identity |
| current candidate nodes | none. **AA1 成像光學元件** is the lens and the optical path; **A4-16 影像 IC (CIS/ISP)** is the sensor and the processor chip; neither is a camera or a recorder as a finished device |
| reason no node | n=1, and the boundary against 車載錄影 (H) and against 智慧城市 integration projects is untested |
| population status | 5484 慧友 has no Core by ruling |
| next audit | sweep 監控／攝影機／錄影／NVR／DVR on revenue items. Split 安防監控整機 from 車載錄影 from 影像 IC before counting |

## Completed mapping history — **not gaps, and no longer outstanding**

A row here is the opposite of a gap: the node exists, its definition already
names the products, and the only thing that was missing is the adjudication.
Keeping such rows in the gap inventory would misreport the tree as incomplete
when what was incomplete is the work.

The distinction, stated once so it survives: **G1's 達明 and 廣運 are gaps** —
the activity is real and no node has been shown to hold it. **G4-01's five and G5's eleven were
backlog** — the node was written, approved, and its declared product list
matched these companies word for word.

**Active mapping backlog = 0.** This section is retained as decision history and
as a regression reference, and is **not** part of the gap population.

### G4-01 傳動與線性運動 — 5 companies, node went n=0 → n=5 on 2026-09-08

| company | FY2025 line | matches G4-01's declared |
|---|---|---|
| 2049 上銀 | 線性滑軌 62.00 ＋ 滾珠螺桿 20.00 | 線性滑軌、滾珠螺桿 |
| 4540 全球傳動 | 滾珠螺桿 48.89 ＋ 線性滑軌 44.03 | 同上 |
| 8083 瑞穎 | 自動倉儲使用之軸承 74.15 | 軸承 |
| 4583 台灣精銳 | 減速機 74.08（另 客房及餐飲服務 18.78） | 減速機 |
| 4571 鈞興-KY | 齒輪箱 34.94 | 齒輪與齒輪箱 |

Opened and closed 2026-09-08. The G15 audit found this cell's apparent
emptiness was a mapping backlog and not a missing role — and thereby weakened
the reading that G4's axis is short a cell.

**These five were never a taxonomy gap.** G4-01 already existed and its
Definition was already approved; the work was mapping completion, not node
creation. Result:

| company | node | role | weight | note |
|---|---|---|---|---|
| 2049 上銀 | G4-01 | **Core** | 82.00 | 線性滑軌 62.00 ＋ 滾珠螺桿 20.00 (R020④). Evidence is `annual_report_prose` — that filing gives its mix in a sentence, not a table. 機器人 10.00% deliberately **not** mapped: below R026 threshold and undividable across G3-01 / A2-09 / 醫療 (R020②) |
| 4540 全球傳動 | G4-01 | **Core** | 99.38 | 滾珠螺桿 ＋ 線性滑軌 ＋ 滾珠花鍵; every revenue line is a declared product of the node |
| 4583 台灣精銳 | G4-01 | **Core** | 74.08 | 減速機. Also mapped to **Y1 飯店 Direct / Major 18.78** (台中日月千禧酒店) |
| 8083 瑞穎 | G4-01 | Major | 95.00 | four bearing lines summed, **not** split by end market. **Core pending (R019)** |
| 4571 鈞興-KY | G4-01 | Major | 73.99 | 齒輪箱 ＋ 全鋼傘齒 ＋ 粉末冶金齒輪 ＋ 全鋼直齒. 精密五金件 17.59% excluded (contains 打擊塊, a power-tool striker, and the filing does not split the line). **Core pending (R019)** |

Two things this round established that outlive it:

- **零組件的 Industry 歸屬以產品功能與機械價值鏈角色判定，不以終端市場判定** — written into G4 and binding on all its children. 瑞穎 (車庫門、手推車軸承) and 鈞興 (電動工具、園林工具齒輪箱) were the stress-test pair; without this ruling the cell would have had to be split by customer application, destroying the functional axis it was built on. Same principle as G5's 「機床 identity 優先於 application」, extended to the component layer.
- **Exposure mapping and Core determination are two stages.** 瑞穎 and 鈞興 carry Major with no Core, which raises `[core]` and `[core_reason]` from 1 to 3. That is the honest state, not a regression — and the two were deliberately *not* topped up to Core just because the cell had filled out.

### G5 工具機 — 11 companies, node went n=1 → n=12 on 2026-09-08

Same shape as G4-01 above and for the same reason: the node's Definition had
been written and approved earlier the same day, and the population audit had
already identified every one of these companies. What was missing was only the
adjudication.

| company | weight | weight_item |
|---|---|---|
| 1583 程泰 | 100.00 | 加工機 ＋ CNC車床 |
| 6606 建德工業 | 96.11 | 磨床及加工中心 |
| 7709 榮田 | 95.87 | 電腦數控車銑複合加工機 ＋ 電腦數控車床 |
| 1530 亞崴 | 94.20 | 龍門型立式綜合加工中心機 ＋ C型立式綜合加工中心機 |
| 1540 喬福 | 90.00 | 電腦車床 ＋ 切削中心機 |
| 4544 春日 | 88.69 | 成型機 ＋ 輾牙機 ＋ 打頭機 |
| 4513 福裕 | 85.81 | 磨床 ＋ CNC磨床 ＋ CNC綜合加工機 ＋ 龍門磨床 ＋ CNC車床 ＋ 立式車床 |
| 1539 巨庭 | 79.90 | 自動鉋木機 ＋ 落地形圓鋸機 ＋ 落地形手動鉋木機 |
| 4562 穎漢 | 72.35 | 全自動整廠設備 ＋ 工具機 |
| 4526 東台 | 70.68 | 綜合加工中心機 ＋ 電腦數值控制車床 ＋ 專用機 |
| 4563 百德 | 61.84 | 綜合加工機 ＋ 航太加工機 |

All eleven are **Core**, and ten of them by R019 branch ① — a single candidate
node, so R023 is never exercised. Only 4526 東台 has a second identified
industry (B5-01 印刷電路板加工機 9.84%, Minor) and therefore carries a written
Core rationale. 1528 恩德 was the node's pre-existing member and is not part of
this backlog; its Core evidence was refreshed under R023-R because the sentence
「本節點目前無 Product Definition」 had gone stale.

**巨庭 and 穎漢 were never in the gap population either.** They lived in G6's
Definition as two of the four cases that tested the G5-before-G6 precedence, both
having passed through G5 and so never reached G6. Leaving them unmapped would
have made them look like a gap on the next coverage audit while the tree already
recorded the ruling.

Four things this round established that outlive it:

- **專用程度不是分類軸** got its second and third instances. 4544 春日 is dedicated
  to a *product* (fasteners), 4526 東台's 專用機 line to a *production pattern*
  (one part, high volume, custom fixturing), 1539 巨庭 to a *material* (wood).
  Three different kinds of specificity, one answer.
- **被加工的材料不構成 G5 的分類軸** — written into the Definition this round.
  巨庭 is the regression case; 1528 恩德 (「適用金屬及非金屬」) shows the node was
  already material-neutral in practice before anyone said so.
- **營收線的名稱不是它的內容.** 4562 穎漢's largest line is called 全自動整廠設備,
  which reads like plant integration (gap G11); its own filing defines the term as
  金屬彎管機、切管機、管端成型機, and the filing's own machine-tool taxonomy lists
  彎管機 under 成型工具機. Mirror image of 6414 樺漢, whose 智慧工廠事業群 line hides
  industrial PCs. Both need the 商品項目 chapter to resolve.
- **A residual percentage never establishes what it contains.** 4563 百德's largest
  line (其他 38.16%) is undisclosed and was left out of every node; 4513 福裕's
  85.81 is quoted as the reproducible sum of its parts precisely because 100 −
  14.18 gives the same number by coincidence.

## G19 · 平板／建築玻璃與容器玻璃（P2 玻璃 has a name and no definition）

| | |
|---|---|
| **gap_type** | `population_pending` — the node **exists**; what is missing is its definition, its children, and the adjudication that would populate them |
| **origin** | 2026-09-11 lifecycle re-adjudication of 1802 台玻. Its `weight_status='Not_Extracted'` was a work order, not a conclusion; the filing-selection fix (F04 → F11) made the annual report readable and the mix table it produced disproved the company's own Core |
| what the filing says | 平板玻璃 59.63% · 玻璃纖維 31.31% · 玻璃器皿 9.06%. R023 ① (所營業務 lists 平板/壓花/鐵絲網/反射/強化膠合/鏡板/複層/軌道車廂/磨邊 as items 1–10, 容器 as 11–12, 玻璃纖維絲及布 as item 13), ④ (計畫開發之新商品: Low-E 鍍膜玻璃, 耐熱奶瓶與高白烈酒瓶, TG 品牌生活器皿 — none is fibreglass) and ⑤ all point at flat glass |
| why B1 cannot hold it | **B1 is PCB材料**. B1-02 玻纖布 is fibreglass cloth as a PCB laminate reinforcement, and it is a correct membership — but it covers 31.31% of the company at most. 平板玻璃 59.63% and 玻璃器皿 9.06% have no cell anywhere under B1 |
| current candidate node | **P2 玻璃** (L2, under P 建材/玻璃/造紙). It carries a `main_products` line — 平板/建築/汽車/光電/特殊玻璃 — and **nothing else**: no definition, no L3 children, no members |
| observed candidates | 1802 台玻 (68.69% of revenue unmapped). The 玻璃陶瓷 listing bucket also holds 1806 冠軍, 1809 中釉, 1810 和成, 1817 凱撒衛, none of them mapped — so this is not a cell for one company, but none of the four has been adjudicated either |
| evidence coverage | 1802 read (F11); the other four not read |
| population status | 1802 台玻 is **Core pending** by ruling. Its B1-02 row stays as Major with `weight_status='Too_Coarse'` |
| why the 31.31% is not a number | the filing separates the uses in prose — 「玻布主要作為印刷電路板(PCB)上游原料」 vs 「玻纖產品之應用以熱塑性用途…」, with the industry diagram sending 玻纖 to 管道業, 桶槽業, 建材業, 傘具業, 運動器材業 and 風力發電葉片業 — but the revenue table merges both into one line and gives no basis for splitting it. Knowing the line contains B1-02 is not knowing B1-02's share (R020②) |
| **do not** | write P2's definition from 台玻's revenue table. That would shape a branch around one company's product mix, which is the R025 failure this file exists to prevent. 平板/建築玻璃, 汽車玻璃, 光電玻璃 and 容器/器皿玻璃 are plausibly different roles, and which of them are cells is a question for the population |
| next audit | adjudicate 1806, 1809, 1810, 1817 and any 玻璃 revenue lines outside the listing bucket; then decide P2's children and scope before giving 1802 a Core |


## G20 · 金融業的分類軸 — **RULED 2026-09-12**：leaf 落在金融功能／子公司型態

| | |
|---|---|
| **gap_type** | `axis_ruled` — the axis is decided; what remains is the node work under it. This was never a missing node: the question was which axis an issuer's disclosure is even *about* |
| **origin** | 2026-09-12. The parser's Gate 1 work produced a contradiction: 2884 玉山金's 利息收入/手續費收入/其他淨收益 table had been called a product mix while 2897 王道銀行's 利息淨收益/手續費淨收益/兌換淨損益 table had been called an income statement. They are the same kind of table. Ruling: settle the taxonomy first, and add no financial vocabulary to Gate 1 until it is settled |
| **the two axes** | **收益類型** — 利息/手續費/金融商品/兌換/其他淨收益. Answers *how income was formed and where accounting puts it*. **業務線** — 企金/個金/財管/金融市場. Answers *what the company sells to whom* |
| **why this is not a parser question** | both axes are tables of revenue percentages on the same page, often adjacent. No numeric, structural or header test separates them, and the issuer does not label one as primary. Only a decision about what the tree is for can choose |
| **the ruling** | see 「裁定」 below. The audit that follows is what moved it: the direction going in was 業務線, and the evidence sent it one level up |

### Audit: which axes do financial issuers actually disclose? (FY2025, 23 issuers attempted, 23 recorded)

Seven of the 23 have no 營業比重 tables at all — 2801 彰銀, 2812 台中銀, 2834 臺企銀,
2836 高雄銀, 2845 遠東銀, 2849 安泰銀, 2887 台新新光金, 5863 瑞興銀, 5876 上海商銀 —
which is a separate discovery gap, not evidence about the axis.

| axis | issuers observed | labels as written |
|---|---|---|
| 證券 brokerage lines | 8 — 2880, 2882, 2885, 2886, 2889, 2890, 2891, 2892 | 經紀 / 自營 / 承銷 (+其他), near-verbatim across all eight |
| 壽險 policy lines | 6 — 2882, 2883, 2885, 2891, 2892, 5880 | 人壽保險 / 傷害險 / 健康險 / 年金險 (+團體, 投資型) |
| 產險 policy lines | 4 — 2880, 2881, 2882, 2886 | 火災 / 水險·運輸 / 車險 / 工程 / 責任 / 傷健 |
| **銀行 business lines** | **7** — 2838, 2880, 2881, 2882, 2891, 2897, and 富邦's HK arm separately | see the mapping below |
| 收益類型 only, no business line | 3 — 2884 玉山金, 2885 元大金, 2886 兆豐金 | 利息收入 / 手續費收入 / 其他淨收益 |
| by subsidiary (holding-company level) | 2880, 2881, 2882, 2885, 2886, 2889, 2891 | 採權益法認列之投資收益 over a list of subsidiary names |

Banking business lines, as each issuer words them:

    corporate   企金授信業務(2897) 企業金融(2880) 企業金融業務(2838) 企金業務(2882)
                法人金融(2881) 法人金融與資本市場(2891)
    consumer    個金放款業務(2897) 消費金融(2880) 消費金融業務(2838) 消金業務(2882)
                個人金融(2881, 2891)
    wealth      財富管理業務(2897, 2882) 信託及財管(2880) 財富信託業務(2838)
    markets     股債投資業務(2897) 金融交易(2880) 金融市場(2881) 投資及其他業務(2838, 2882)

### What the audit settles, and what it does not

**Settled.** The first three banking columns are the same economic activity under
different words, across six issuers — that is a cross-issuer identity, not one
issuer's internal segmentation. The fourth (markets/investment) is looser: 金融交易,
金融市場 and 投資及其他業務 are not obviously the same scope.

**Also settled, and it cuts the other way.** The banking business line is
disclosed by 7 issuers; 經紀/自營/承銷 by 8, and it is the *more* consistent axis.
The strongest cross-issuer identities in this sector are at the **subsidiary-type**
level — securities, life, non-life — not at the bank-business-line level.

**Not settled.** Three issuers, 玉山金 among them, disclose **no** business line at
all. An axis that a third of the population does not report cannot be the sole
basis of a leaf without leaving those issuers unclassifiable on their own filings.
2881 富邦金's Hong Kong arm uses 零售銀行 / 企業及機構銀行 / 商業銀行 / 資金部 /
市場部 — a different segmentation in a different jurisdiction, which is the
issuer-specific risk the ruling warned about, though it is a separate legal entity.

### Do not

- do not write a Gate 1 vocabulary that rejects 利息淨收益-style tables. Under the
  direction above they are an attribute rather than a mix, but 玉山金 has nothing
  else, so rejecting them turns a disclosed number into `no_section`
- do not read a financial holding company's mix from one subsidiary. `parse_pdf`
  currently returns 2882 國泰金's **life-insurance** mix (人壽保險 51.2 …) because
  that table comes first; the bank, non-life, securities and asset-management
  tables on the following pages are not consulted. The number is real and its
  scope is one subsidiary, which is not the group
- do not treat the 7 issuers with no 營業比重 table as evidence about the axis

### 裁定（2026-09-12）

> 金融業 Industry leaf 的**預設軸**為可跨 issuer 驗證之金融功能／子公司型態；收益類型
> 與 issuer-specific 的銀行業務線，不得僅因存在 revenue percentage 而升格為 Industry
> leaf。後者僅於形成跨 issuer、一致、可定義且可驗證的 economic territory 後，方可提出
> 升格。

**預設軸, not 唯一軸.** The evidence settles how to classify the population that
exists now. It does not establish that financial taxonomy must stay at this level
forever, and the sentence is written so a future promotion has somewhere to start.

Three levels, and only the first is an industry axis:

    金融功能 / 子公司型態     銀行 · 證券 · 壽險 · 產險 · 投信 · 票券      ← Industry leaf
    業務線                   企金 · 個金 · 財管 · …                      ← business mix / attribute
    收益類型                 利息 · 手續費 · 金融商品損益 · 其他淨收益      ← accounting axis, never an industry axis

**Why the middle level does not rise.** 經紀/自營/承銷 is the most consistent
vocabulary in the whole sector — eight issuers, near-verbatim — and consistency is
exactly what would tempt a promotion. It stays an attribute anyway: it is how a
securities firm reports itself, not a separate economic territory from 證券業.
Consistency of wording is not evidence of a distinct industry.

**Why the bottom level does not rise.** An issuer's income types are a true and
correct disclosure. Being true is not the test. A table earns industry evidence by
sitting on an established economic-identity axis, and 利息收入/手續費收入 does not.

**What this resolves.** 2897 王道銀行 and 2884 玉山金 stop contradicting each other:
both issuers' income-type tables are real disclosures, and neither is an industry
mix. The parser question — which table `parse_pdf` returns — is downstream of this
and is not allowed to decide it.

### Next audit

1. re-check the existing membership of 2897 and 2884 against the ruling, and of
   every financial issuer already carrying an exposure
2. settle how a holding company with several subsidiary types is exposed. 2882
   國泰金 holds 壽險 · 銀行 · 產險 · 證券 · 投信 and the filing gives each
   subsidiary's own mix but no share of the group, so R020② applies directly:
   knowing a line exists is not knowing its share
3. only then give Gate 1 whatever vocabulary follows from the ruling. The parser
   implements the taxonomy; it does not get to shape it by being easier to write
   one way than another

## G21 · 設備廠自售零配件與耗材（machine makers' own spare parts and consumables）

| | |
|---|---|
| **gap_type** | `population_pending` — two observed makers; whether a machine maker's own spare parts have a cell has not been put to the R025 test |
| observed candidates | 2: **1528 恩德** — 零配件 inside the undivided 機械 73.15% line (「曲面磨床及相關刀具、零配件」); **4744 皇將** — 其他零件 22.91% as a line of its own (「主係應用於包裝設備之售後服務備用與更換用之零組件及耗材」, and the same wording for its softgel machines) |
| evidence coverage | FY2025 annual-report product tables, read 2026-10-23 |
| current candidate nodes | none holds them. G5, G6-02 and G6-04 take machines delivered whole (G5: 「G4 是零組件、G5 與 G6 都是整機」; G6-02／G6-04: 「以整機或整線形式交付」). G4 is the component branch, but its leaves are functions — G4-01 transmission and linear motion, G4-02 flow control, G4-03 cutting and grinding tools — and neither filing says what the parts do. G4's own Definition lists 「其他工業機械零組件」 as a candidate L3 that has not been built |
| reason no node | the parts are sold by the machine's maker for machines it sold, so two answers are open: they follow the machine's node, or they belong on a G4 component leaf that does not exist yet. R020 ②-MX (10) MX-INC excuses only installation, commissioning, warranty and repair services, not parts, so it does not settle this |
| population status | 1528: 零配件 recorded as MX-SKIP-UNDECIDED (no cell) on the 機械 line, which therefore carries `Granularity_Insufficient` on G5, B5-01, B5-02 and G4-03. 4744: 其他零件 has no row |
| next audit | none scheduled. While this row is open, 1528's four rows stay `Granularity_Insufficient`, the state R020 ②-MX (7) gives the known nodes of a line that holds a component with no cell. Only once this row closes, whichever way, does every component of the 機械 line name a node; the line is then MX-FULL and the rows converge to `Too_Coarse` under the same clause (「未定組成日後裁定，依 ①-S 收斂條件轉為 TOO_COARSE 或 WEIGHTED」). Closing it is a separate ruling and write, not part of any current one. "Follow the machine" would also put 4744's 其他零件 on G6-02 and G6-04 as one mixed line |

## G22 · 真空微波管與真空電子元件（magnetrons, travelling-wave tubes and other vacuum microwave tubes）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — a component maker's product with no cell; two observed sightings, one of them the subsidiary of the other |
| observed candidates | 2: **6895 宏碩系統** — 軍用真空微波管 5.08% (「應用於國防之磁控管或行波管」), and 商用真空微波管 7.94%, whose UV-curing tubes are 「方面其是半導體製程中紫外線固化設備裡的重要零組件」; **6698 旭暉應材** — the same tubes inside its line 「(4)《AI液冷零組件暨真空微波管與半導體設備零組件》詳本公司子公司-宏碩系統(股票代碼:」, which is 6895's revenue |
| evidence coverage | FY2025 annual reports, read 2026-10-24 (P4 C-group batch 1) |
| current candidate nodes | none for the military tubes: S3-02 takes radar and electronic-warfare systems, not the tubes inside them, and S3-01's 軍用電子 names no vacuum electron device. The commercial UV-curing tubes have one candidate, A2-10, unproven: the filing calls them consumables, also sells them for solar, and does not name the customer |
| population status | 6895: 軍用 NO_NODE, 商用 UNCERTAIN_FIT (candidate A2-10), and the 商用 line makes 6895's A2-10 row an unproven-line row (R020 ②-MX (7)(iii), Granularity_Insufficient). 6698: MX-SKIP-UNDECIDED (a) on the 73.55% line, which is therefore MX-OPEN |
| next audit | sweep 微波管／磁控管／行波管／magnetron on revenue items and business prose |

## G23 · OLED 蒸鍍用精密金屬遮罩（fine metal masks for OLED evaporation）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one observed maker |
| observed candidates | 1: **6698 旭暉應材** — 金屬遮罩 13.77% (「主要應用於OLED面板發光染料色素蒸鍍製程使用之金屬遮罩,需要」) |
| evidence coverage | FY2025 annual report, read 2026-10-24 |
| current candidate nodes | none: A1-10 takes semiconductor photomasks, E7 takes panel equipment delivered whole, E2-02 takes backlight optical films |
| population status | 6698: NO_NODE, no row |
| next audit | sweep 金屬遮罩／FMM／蒸鍍遮罩 |

## G24 · 製鞋機械（footwear-making machinery）

| | |
|---|---|
| **gap_type** | `taxonomy_node_candidate` — one observed maker |
| observed candidates | 1: **8383 千附** — 機械收入 7.2%, its own-brand line (「以”全鋒Chenfeng”自有品牌生產及銷售製鞋產業相關機械,包含結幫機、壓底機、」) |
| evidence coverage | FY2025 annual report, read 2026-10-24 |
| current candidate nodes | none: G5 takes machines with a machine-tool identity; G6-01 to G6-04 take plastics and rubber, packaging, food and pharmaceutical machinery |
| population status | 8383: NO_NODE, no row |
| next audit | sweep 鞋機／製鞋機械 |

## G25 · 模具與治具（moulds and fixtures for semiconductor packaging and test）

| | |
|---|---|
| **gap_type** | `population_pending` — the family is already open as a case (`tmp/rejudge/rejudge73/n4_r023r_jig_family.md`, 7769／3178 治具, OPEN since 2026-09-18); this row records it in the gap register and adds the mould sighting |
| observed candidates | 3: **8092 建暐** — 精密模具 9% (「本公司及子公司之部分模具產品係屬半導體構裝及測試製程之用,而半」); **3178 公準** — IC 封裝工模夾治具 inside 半導體類 62.27%; **7769 鴻勁** — 治具及各類模組 16.68% (case file) |
| evidence coverage | FY2025 annual reports, read 2026-09-18 (case file) and 2026-10-24 |
| current candidate nodes | A2-10 only, and only by delivery: its definition says moulds, fixtures and test-interface products are not drawn in merely because they go to an OSAT or serve back-end equipment. No cell takes moulds as such |
| population status | 8092: NO_NODE, no row (its A2-10 row is on HOLD for a separate reason). 3178: MX-SKIP-UNDECIDED (b), candidate A2-10, on an MX-OPEN line. 7769: per the case file |
| next audit | with the case file; sweep 模具／治具 on revenue items |
