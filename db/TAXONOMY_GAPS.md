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
| where the rest went | 光通訊薄膜濾光片 2 家 → **D6-01**（統新 99.98、東典 75.29，兩家的商品項目都明寫「光通訊」）· 半導體晶圓級光學鍍膜 → **A3-05**（澤米 54.05）· 隱形眼鏡 2 家 → **N3**，定義依 R025 放寬（精華 99.87、望隼 99.65）· 車用感測與車用光學射出件 → H · 金屬沖壓 → F4-01 · 儲存媒體 → 見 G8 |
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
  it spans imaging optics, AR waveguides and material processing. Its Core sits
  on A3-05; this line is not assigned rather than assigned to the largest-looking
  candidate
