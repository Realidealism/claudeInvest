# P22 repair — revision 2, after source review of the second differential run (design for ruling)

**Status:** design accepted for implementation, pending the scan-dependent decisions (ruled
2026-09-15): R2-7 and R2-2d adopted; R2-2c and R2-6 wait for their scans; the other contracts stay as
designed and are not widened. Not yet: parser commit, a 2302 rerun, reconciliation v3, a manifest, DB
writes. The parser is unchanged since the second run. DB 0 writes,
no manifest, scheduler `InvestAnnualReportSweep` Disabled, parser uncommitted, reconciliation v3 not
started. P22 source verification stays CLOSED; the second differential run is **STOP** and cannot
pass while any REVIEW item is rejected.

## Why this exists

The second run (`tmp/rejudge/p22r2/`) passed closure, identities, determinism 65/65, K 20/20 and
refusal accounting (after ruling A). Source reading of the 142 REVIEW companies
(`tmp/rejudge/p22r2/review/verdicts.tsv`, page and reason per company) gave **118 ACCEPT and 24
REJECT**. 2349's `measurement_error` was a harness defect (a sort comparing `None` with a float), fixed
in `run.py` and closed by a single-company rerun (ruled 2026-09-15: tooling, not parser; no 2302 rerun).

Ruled 2026-09-15: the 8 revenue losses are **regressions** (contract-induced), not an acceptable NULL;
the other 16 rejections enter this revision too, kept apart by mechanism.

**The goal of revision 2 is that output matches the printed evidence — not that the 24 companies
produce output again.** Every mechanism below was traced on the company's own table or lines
(`tmp/rejudge/p22r2/r2_scopes.py`: column scopes, selected columns, caption, table scope).

## Principles that do not move

1. No position fallback: unknown is not the first column, the first amount, consolidated or current.
2. Nothing is borrowed across periods or entities (P22-4). A value of another stated period or stated
   entity is never taken.
3. A refusal stays terminal against weaker readers (D1).
4. Non-revenue qualification disqualifies only; no heading is ever required to say 營業比重 (R1-1).
5. A plausible total (50–150) is not proof of a complete read (P27).
6. K stays the 20 prior rulings. The revision's fixtures are tests, not K. The next full run compares
   with frozen v2.

---

## R2-1 · Revenue preservation — 8 (1737, 1795, 2374, 3055, 4534, 4741, 9928, 8411)

In all eight the source prints the fiscal year's amount in the same table or line as the share, and v2's
value matched it. The share is read; the amount is dropped.

**Mechanism A — table, period named (1795, 4741).** Header 「114年度 | 金額 | 佔合併營收比重」. The share
column's label carries 合併, so its entity is consolidated; the amount column's label carries no entity.
`_select_columns` keeps only columns of the chosen entity (`cols = [s.col … if s.entity == chosen.entity]`),
which removes the amount column.

**Mechanism B — table, no period on the columns (2374, 9928, 3055, 4534, 1737).** The year sits in the
label column (「114年度」 over 產品別) or in the heading (「2025年度主要產品之營業比重」), so both columns are
unlabelled. The unlabelled branch looks for an amount only when the chosen share has an entity; an
entity-less share never gets its amount.

**Mechanism C — line, quantity beside the amount (8411).** 「馬 口 鐵 罐 2,694,259 29.33% 支 1,066,874」
under 「合併營收金額 營收比重」 and 「數量 銷售數量 / 單位 (仟)」. Two numbers above 100 on the line; D4 sets
revenue NULL. The second is a count: the unit 支 / 張 / 片 / 噸 stands right before it.

**Proposed contract R2-1.**
- *Table.* The share's amount is the one amount column in the same table whose identity does not
  contradict the share's: where both state a period it is the same period; where both state an entity it
  is the same entity; an attribute the amount column leaves unstated does not contradict. Exactly one
  such column, otherwise NULL (D4 unchanged). In the unlabelled branch this applies only after the share
  column was accepted as the table's unique target evidence.
- *Line.* On a line whose header names a quantity (數量 / 銷售數量 / 產量 / 銷量), a number written next
  to a count unit (支 張 片 噸 公噸 個 台 套 條 箱 瓶 …) is a quantity, not an amount; revenue is then the
  single remaining amount under D4.
- *Never:* an amount column of another stated period or entity; a choice between two compatible amount
  columns; magnitude deciding what a number is.
- *Ruled reading (2026-09-15):* an unknown attribute is no conflict; an established attribute must match;
  the candidate amount column must still be unique. Unknown is not any value: a share of 114 / 合併 pairs
  with an amount of 114 / entity unstated, never with an amount of 113 / entity unstated.

## R2-2 · Owner selection — 4 (5522, 3705, 2884, 2881)

Wrong evidence would be written, so this group comes first.

**5522 — a ranking table.** Page 90 is headed 「依…建材營造類之合併營業收入排行…本公司114年度位列第2名」;
its rows are other companies and its percentage column is the rank (欣陸 1, 遠雄建設 2…). 「合併營業收入」
made its table scope consolidated, so it outranked page 87's unlabelled real mix.
*Contract R2-2a:* 排行 / 排名 / 名次 join the R1-1 non-revenue heading and header words (disqualification
only).

**2884 — a footnote read as the next table's caption.** 「註1：表列數字係玉山銀行合併之資料。 B. 營業比重」:
the footnote closes the table above and the 合併 in it made the investment-trust subsidiary's table below
the consolidated owner.
*Contract R2-2b:* a caption line opening with a footnote marker (註 / 註1 / 註: / (註)) belongs to the table
above and is not read for the table-level entity of the table below. After it, 2884 refuses as a financial
holding (it also carries its own HOLD ruling).

**3705 — a foreign subsidiary's table.** Page 56 「(2)營業比重： 單位：日圓仟元；%」 is the Japanese
subsidiary; its 2025年度 label beat page 55's unlabelled company mix (rule 2).
*Contract R2-2c:* a table whose unit note names a currency other than the reporting currency (日圓 / 美元 /
港幣 / 人民幣 / 歐元 / USD / JPY / HKD / RMB …) is a foreign operation's statement, not the company's
consolidated composition, and is not a candidate. **Scan required before ruling**: companies whose own
annual report presents amounts in a foreign currency would lose their mix; the count of candidates with
such unit notes across 2302 filings must be known first. Also acts on 6023 (美元仟元 subsidiary) and
2881 (港幣 table).

**2881 — a subsidiary's own consolidated basis.** 「(2) 營業比重(採合併基礎)」 is 富邦產險's section. The
entity word is true of the subsidiary, not of the holding. No lexical rule separates it without reading
section structure.
*Ruled 2026-09-15:* no parser special case; 2881 stays under `HOLD_SCOPE` with the financial holdings
(2880, 2882, 2885, 2886, 2889, 2891), pending a separate holding / subsidiary evidence-identity rule. 「採合併
基礎」 describes the subsidiary's accounting basis, not the evidence's identity. The HOLD is not a parser
failure to be forced into automatic resolution. The principle stands: an entity word
establishes the reporting company's scope only when it qualifies that company.

## R2-3 · Period selection — 2 (6827, 6876)

Header 「113年度 | | 114年度 |」 over 「銷售金額 營業比重 銷售金額 營業比重」; the 114 cells are all 「-」 (no
revenue). `_column_scopes` produced two scopes for four columns — the 114 columns hold no numbers and were
not counted — and the 113 share column took the 「114年度」 label. The 113 share (100%) was read as 114.

5609 has the same mechanism (its amounts are text, 「188億0仟4佰萬元」, so the amount columns were not
counted and both % columns took 113), and is listed under R2-5.

**Proposed contract R2-3.** Column identity is built over the header's columns: every column the deepest
header row names is a column, whether its cells hold numbers, placeholders or text. A target column whose
cells are all placeholders gives NULL shares — the rows are skipped, as R1-3′ does on lines.
*Never:* a year label moved onto a neighbouring column because its own column is empty.
Expected: 6827 and 6876 no rows; 5609 reads 114's shares (空運 64.1 / 海運 30.8 / 合同物流 5.1) with
revenue NULL (the amounts are not numbers).

## R2-4 · Non-revenue competition — 4 (2105, 4142, 2449, 2929)

**2105 — production and sales volumes.** 「1.產銷情形： 單位：仟條；%」 with 「114年度生產 | 114年度銷售 |
113年度銷售 | 增減比例」 refused (two target columns), and the refusal blocked page 77's real mix.
**4142 — market share in doses.** 「(1)四價流感疫苗市場銷售狀況 單位：萬劑」 with 「數量 | 佔有率」 became a
candidate beside page 88's mix. R1-1's header list has 市占率 / 市場占有率 but not bare 佔有率, nor 數量.
*Contract R2-4a:* a table is non-revenue when its unit note names only count units and no currency
(仟條, 萬劑, 公噸, 支 …), or its header names 生產, 數量, or a bare 佔有率 / 占有率 (disqualification only).

**2449 — a region trend table.** Page 88: rows 民國110年 … 民國114年, columns 內銷比重(%) | 外銷比重 — a region
split over years, no item rows. It refused (region columns without a total) and blocked the line reader's
real mix (晶圓測試 30.56 / 積體電路測試 66.43 / 其他 3.01).
*Contract R2-4b:* a table whose row labels are periods and whose every share column is a region (內銷 / 外銷
/ 地區) is a region axis, non-revenue, and not a refusal. Narrow on purpose: 1307 and 1708 (item rows under
內銷 / 外銷 columns, accepted refusals) are unchanged. *Ruled:* not to be widened to any table naming a
region.

**2929 — a change rate read as a share.** Lines under 「2024年度 2025年度 增(減)比(%)」: 「童裝 2,065,430
1,668,125 -19.24%」. The line reader counted -19.24% as a share and refused, which blocked the table's
amounts-only read.
*Contract R2-4c:* where a line header names a change measure (增(減)比 / 增減 / 成長率 / 變動) and no share,
% values on its lines are changes: those lines are not mix lines and do not refuse.

## R2-5 · Header / value boundary — 2 (3665, 5609)

**3665.** The line 「比重 比重 5」 — a header line with a stray page number — was read as a value line and
refused; the table under it is clean (114: 資訊科技與數據傳播 42.4% …).
*Contract R2-5:* a line whose label is made only of header words (比重, 金額, 營業比重, 百分比, 年度, 產品名稱 …)
is a header line; its numbers are neither values nor refusals.

**5609.** See R2-3.

## R2-6 · No revenue, no rows — 2 (3042, 6709)

3042's window holds a product-dimension table (「SMD Seam Sealing 3.2 x 2.5mm」); 6709 states 「近三年尚無營業
收入」 and then a trial description (「LTh(K) 20、40、60 微克」). Both refused; the right output is no rows.
Both write nothing either way, so the severity is low.
*Contract R2-6:* a number followed directly by a unit of measure (mm, cm, 吋, 微克, mg, g, kg, 組, 週, 周,
天, 人, GB, TB, W, V, Hz …) is neither a share nor an amount. **Scan required before ruling** for unit
words sitting after genuine values in real mix lines.

## R2-7 · Measurement label contradicted by data — 1 (1467)

「產品名稱 | 營業比重 | 百分比(%)」: the column labelled 營業比重 holds 604,445 … 合計 5,149,512 and the one
labelled 百分比 holds 11.74 … 100.00. Two share labels; P22-2 refused (two share columns, no period or entity).
*Contract R2-7:* a share label whose own column holds values above 100 is contradicted by its data and is
not a share; with one share column left, it is read. The contradicted column's measure is unknown, so
revenue is NULL.
*Ruled 2026-09-15:* adopted; 1467 keeps revenue NULL, recorded as P26 / measurement-label conflict. This is
not R2-1: there an amount is established by identity and only an attribute is unstated; here the label
contradicts its data and nothing independent establishes the column. A label is not unconditionally true,
and a label is not inferred back from magnitude.

## R2-8 · Prose leakage — 1 (4108)

「2.營業比重」 says only 「114 年度合併營收為新台幣 134,576 仟元」; the prose reader read 「E.懷特胺速®:含有
99.7%高純度左旋麩醯胺酸」 under 「3.公司目前之商品(服務)項目」.
*Contract R2-8:* the prose reader's window ends at the first value-free section heading after 營業比重, the
same boundary L1 uses for lines. 4108 then has no rows.

---

## Verification

1. Real fixtures for every case, captured from the cached filings with caption, caption lines and heading
   (tables) or page text (lines) — `tmp/rejudge/p22r2/add_r2_fixtures.py`. Negative controls: 1307 and
   1708 (still refuse), 2601, 6496, a financial holding, 6023 (after R2-2c its own table owns), 2332.
2. Scans before ruling on R2-2c (foreign currency units) and R2-6 (units after values), counts reported.
3. After ruling: implementation with tests over those fixtures; the full suite green is necessary, not
   sufficient.
4. Then the third full differential run, same design and gates, baseline frozen v2, into a new directory.

## Scan results for R2-2c and R2-6 (2026-09-15)

`tmp/rejudge/p22r2/r2_scan.py` over all 2302 cached filings (closed; 3308 and 6209 unreadable PDFs,
recorded), summary `r2_scan_summary.txt`.

**R2-2c — foreign currency.** 29 companies have a table whose caption names a foreign currency; in 5 of
them that table is a candidate:

| company | foreign candidate | other candidates | outcome under the rule |
|---|---|---|---|
| 6023 | p142 港幣仟元, p144 美元仟元 | 1 (p133, 仟元) | p133, the company's own table, is the unique candidate |
| 3705 | p56 日圓仟元 | 1 (p55) | p55, the company's own mix, is the unique candidate |
| 2881 | p187 港幣百萬元 | 10 | financial holding, HOLD_SCOPE (R2-2d) |
| 8436 | p104 「市場占有率 單位：億美元」 | 1 | already non-revenue by R1-1 (市場占有率) |
| **4162** | p120 — **false positive** | **0** | its caption says 「美金 200 仟元(約新台幣 6,521 仟元)…」 but its unit note is 「單位: 新台幣仟元」; read on the caption text, 4162 would lose its only mix |

*Revised R2-2c′:* the currency is read from the **unit note** only — the text after the last 單位： / 單位: in
the caption — and a table is disqualified only when that note names a foreign currency and not
新台幣 / 台幣 / NTD. Narrative money in a caption is not a unit. Under R2-2c′ no company's only candidate
is lost: 4162 is untouched; 6023 and 3705 keep their own table as the unique candidate.

**R2-6 — unit-suffixed numbers.** Across 2302 filings only 5 window lines carry both a value-shaped token
and a unit-suffixed number (6854, 2349, 6141, 7556, 4532), all prose or specifications, none a row the
parser reads. No counterexample. **But R2-6 as designed does not fix its own cases**: in 3042's
「3.2 x 2.5mm」 only 2.5 carries the unit, so 3.2 stays a value; in 6709 the unit 微克 is split across a
line break (「20、40、60 微 / 克」).

*Revised R2-6b (replaces R2-6):* a line block holds share evidence only if a line the reader takes as a
value line carries a %-marked number, or the header above its first value line names a share (比重 / 比例 /
比率 / 百分比 / 占 / 佔 / %, compared without spaces; on the opening line only what follows 營業比重 counts,
e.g. 1708's 「營業比重(%)」). Without share evidence the block's lines neither become rows nor refuse.
Measured on the 139 line-reader companies (`r2_6_measure.py`, `r2_6_measure.txt`): 135 pages unchanged,
**0 pages losing rows**, 3 refusals cleared — 3042 and 6709 (the cases) and 1460 (its refusal came from the
header line 「主 要 產 品 1 1 4年 營 業 比 重」 read as data; 1460 already reads through its amounts-only
table under L1). The three line-reader companies of the second run outside those 139 (2399, 2910, 3443)
all carry % values.

## Decisions to rule

| id | question | recommended |
|---|---|---|
| R2-1 | amount partner by non-contradicting identity; count units mark quantities on lines | yes |
| R2-2a | 排行 / 排名 / 名次 are non-revenue words | yes |
| R2-2b | footnote caption lines belong to the table above | yes |
| R2-2c′ | a foreign currency named in the unit note (not caption narrative) disqualifies a table | yes — no unique candidate lost; 4162 false positive avoided |
| R2-2d | 2881 to HOLD_SCOPE, no parser rule | **ruled: adopted** |
| R2-3 | column identity over header columns; all-placeholder target → no share | yes |
| R2-4a | count-unit / 生產 / 數量 / bare 佔有率 tables are non-revenue | yes |
| R2-4b | period-row × region-column tables are non-revenue (narrow) | yes |
| R2-4c | change-measure headers: % values are not shares | yes |
| R2-5 | header-word-only lines are header lines | yes |
| R2-6b | a line block without share evidence (no % value, no share word in its header) neither reads nor refuses; replaces R2-6 | yes — 0 rows lost, clears 3042 / 6709 |
| R2-7 | data-contradicted share label is not a share; revenue NULL | **ruled: adopted** (P26) |
| R2-8 | prose window ends at the next section heading | yes |

---

## Implementation (2026-09-15)

All contracts ruled (R2-2c′ and R2-6b adopted after their scans). Parser backup before revision 2:
`tmp/rejudge/annual_report_revenue.py.bak-r2`; patches `tmp/rejudge/p22r2/_patch_r2*.py`. Tests:
`tests/test_revision_2.py` (34, over real fixtures; mix_sections 56, mix_lines 35); suite 922 green.

Found while implementing, and handled:
- **R2-6b as measured.** The first implementation took the header to be the lines above the first
  line `_is_value_line` guesses; on 8112's interleaved page 「1. 產業之現況與發展」 comes before
  「產品別 銷貨收入 比重(%)」 and the mix was emptied (an existing test caught it). The header is now the
  lines above the first line the reader acts on (a row or a refusal note), as `r2_6_measure.py` measured.
- **2929.** The unit note 「單位:新台幣仟元;%」 counted as a share word and hid the change header; unit notes
  are left out of the change test.
- **1467 — ruled 2026-09-15: stays refused.** The block opens 「2.一一三年主要產品佔營業比重」 and the window
  starts at 營業比重, which cut 113 off. With R2-7 the table no longer refused, P22-3 left it out as another
  period, and the line reader took 604,445 from it. P22-1 is applied to the opening line: when it names
  periods, none of them the fiscal year, and no header line names a period, the block refuses (no target
  period). R2-7 resolves the mislabelled share column only; it does not license taking another period.
  Not a regression of revision 2.
- **5609 — ruled 2026-09-15: not repaired in this revision.** R2-3 identifies its 114 columns, but its total
  row is labelled 「集團營業額」, is not recognised as a total, and makes the table sum 200; the line reader
  then refuses on amounts written in words. It refused before revision 2 too. Listed for a later ruling;
  revision 2 is not widened for it.

Real-filing check (`parse_pdf`, the 24 and controls) matched the expected outputs above, with 2881 still
reading a subsidiary table (HOLD_SCOPE, R2-2d). These readings are not evidence for the third differential
run, which verifies every change again against frozen v2 (`tmp/rejudge/p22r3/`, same gates, K 20, the
ruled-refusal registry; `run.py` also traces unit-note exclusions for E).

## Third run and R2-8's scope (2026-09-15)

The third run (`tmp/rejudge/p22r3/`) passed every gate (closure, identities, determinism 65/65, K 20/20,
refusal reasons attributed, refusals outside R 8 vs inside 20, 259 to read) with 10 REGRESSION. Nine were
the prose reader (1225, 1308, 1784, 1907, 2353, 2524, 3006, 6492, 8390), unattributed because the
instrument had no prose trace; 1460 was an evidence-path change with an unchanged value.

Read at the source: **an implementation regression of R2-8, not a design failure** (ruled 2026-09-15). The
implementation (1) cut pages with no 營業比重 at their first heading — 3006 lost 「積體電路(IC)銷售收入…」,
8390 lost 「金及含金混合貴金屬佔整體營收比重為 39%」, 1784 gained junk; and (2) took 「(1)」 list items for
section ends — 2524 lost 「住宅大樓出售占 90.41%…」 under 「(3)」, and 1308 was cut at 「(1)」.

Corrected to the ruled scope: the window exists only on a page containing 營業比重, and ends only at a
section heading of the same or a higher level (「3.」「3、」「三、」「(三)」), never at 「(1)」 or 「A.」. Prose now
differs from the pre-revision reader in 4108 (the case) and 1907 only. Tests over 3006, 8390, 2524, 1308
(real pages) added; suite 927 green. The instrument gains the evidence-path trace (`lines_path`,
`prose_path`: the pre-repair reader on the same text) so a mechanism taking over the evidence path counts
as E even when the final value is unchanged. The 249 REVIEW items of the third run were not read; the
fourth run (`tmp/rejudge/p22r4/`, same gates and baseline) replaces it.
