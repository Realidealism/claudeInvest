# P22 repair — revision 1, after the differential run stopped (design for review)

**Status:** ruled 2026-09-14 (R1-1, R1-2, R1-3 with the no-borrowing limit, R1-5; R1-4 held
until 1325 / 1522 / 6547 are read). Implementation notes at the end. The differential rerun has
not started.

## Why this exists

The run (`db/P22_DIFF_RUN_DESIGN.md`) passed every check up to refusals: closure, identities,
determinism 65/65, **K 20/20 confirmed**, and zero unmapped reasons. It stopped on
`CONTRACT_TOO_STRICT`: 65 refusals outside R against 32 inside, and 36 of P22-3's 45 outside R.
No source reading was started.

The diagnosis below comes only from what the run recorded (refusal reasons, header text, table
captions). **It does not say the uniqueness principle is wrong.** It says the contract is
applied to evidence it should never have been given, and in two places it treats a
presentation detail as a scope fact. Every count below is computed from the run's records.

---

## R1-1 · Tables that are not a revenue composition take part in the contract

**Mechanism.** `_rows_from_table`'s gates let through tables such as 主要銷貨客戶, 主要供應商,
銷售地區, 市場占有率, 產銷量值, 預計數量 and 研究發展人員. Under first-table-wins that did no harm,
because the 營業比重 table usually came first. Under P22-3 such a table becomes a competitor,
which yields a "different evidence" refusal. Under P22-1 and P22-2 it becomes a refusal, and a
refusal is terminal, so it can block the line reader that would have read the real mix.

**Counts.** 27 of P22-3's 45 refusals have a candidate whose caption names one of those axes.
8 other refusals have a header that is itself a quantity, a value or a market share: 5452, 5529,
1708, 6696, 5211, 6617, 1314 and 2355.

**Rule (proposed).** A table is **not a revenue-composition candidate** when its table-local
caption, or its header, names a non-revenue axis or measure. Such a table produces no
candidate and **no refusal**. This is R020 ①-C applied at table level, the same qualification
`share_of = other` already applies at column level.

- caption words: 客戶 · 供應商 · 進貨 · 銷售地區 · 提供地區 · 銷售區域 · 地域別 · 分佈區域 ·
  市場占有率 / 市場佔有率 / 占有率 · 研究發展 · 銷售對象 · 預計 · 流動性 · 現金流量
- header measures: 產量 · 銷量 · 需求量 · 銷售量 · 市占率 · 預計數量 · 複合年成長率, and a
  quantity or value column pair (量 | 值)

**Only disqualification, never a requirement.** A table whose caption says nothing is still a
candidate. A caption is not required to say 營業比重: a missing caption must not cost a real mix.

**What it cannot fix.** Where two real mix tables with different axes remain (6496: 藥品分類
and 使用科別), the refusal stands; that is a genuine choice. The 6 financial holding companies
printing one mix per subsidiary (2880, 2882, 2885, 2886, 2889, 2891) stay refused, correctly,
and sit under `HOLD_SCOPE` anyway.

## R1-2 · Table-level part-scope words are too broad

**Mechanism.** At table level, 部門, 內銷 and 外銷 mark a caption part-scoped. Captions use
them in sentences naming a classification axis or a market: 「由於產品營收…以部門別為歸屬」
(8150), 「按各部門分類之營業比重」 (1709), 「以內銷市場為主，比重佔100%」 (2538, 2534),
「國內外銷售」 (6657), 「主要部門別營收與佔比」 (5903). Each became a D2 refusal. Only 2601
(「營業比重（%） 個體」) states a parent-only scope.

**Rule (proposed).** At table level, only **個體** and **母公司** mark part scope. 部門, 內銷 and
外銷 remain column-level entity words, where they sit in a header over a value column and do
name a scope.

**Counts.** Of the 7 table-level D2 refusals, 6 would no longer refuse on this ground; 2601
keeps its refusal.

## R1-3 · The line reader's alignment ignores placeholders

**Mechanism.** A line aligns only when its shares number exactly the groups. A row printing
「-」 for one period (a product new in 114, or discontinued) carries one share for two groups, so
the whole section refuses: 6921, 3689, 7853, 7912, 6735, 6787, 2072 and 3176 all head their
lines 年度 113年度 114年度 / 金額 比重 金額 比重.

**Counts.** 19 line-alignment refusals; 11 of them name both an amount and a share in the
header.

**Rule (proposed).** A standalone 「-」 / 「－」 / 「—」 is a **placeholder value**. It occupies a
position and carries NULL. Alignment runs over value positions, placeholders included:

- positions = 2 × groups → group *i* is positions (2*i*, 2*i*+1). Within the pair, each value's
  form decides its measure (% or ≤100 is a share, >100 is an amount, a placeholder is NULL).
  When both values of the pair are ≤100 unmarked, the measure is undecidable and the line
  refuses.
- positions = groups → every position is a share (or a placeholder).
- any other count → the line refuses, as now.

The target group's share being a placeholder means the item has no share for the fiscal year:
the row is skipped, not borrowed from another group.

## R1-4 · A single period named on a line header

**Mechanism, unconfirmed.** `_line_periods` declares groups only when a header line names **two
or more** periods. 1325 heads 「營業比重:(114年度)」 then 「主要產品項目 營業額 佔營收比重」, and
1522 and 6547 are similar. Why two shares appear on their lines is not visible in the records.

**Proposal.** No rule yet. Read 1325, 1522 and 6547 at the source when their fixtures are
captured. A rule is proposed only after that reading, and ruled separately.

## R1-5 · E missed one mechanism, and two regressions are unattributed

**Mechanism.** `acted()` counts P22-3 only when it owns with 2+ candidates, or owns a
non-first candidate. It misses `_own_section` returning **none** while candidates exist, which
is P22-3 excluding a candidate, for instance one whose caption names another period. 1338 was
classed `REGRESSION` for that reason; v2 read its 資本支出 table, which was wrong anyway. 2548
(`parse_conflict` → `no_section`, formerly `table-amounts`) has no trace at all.

**Proposal.**
- E also counts `_own_section` returning none with one or more candidates.
- The trace records the amounts-only fallback. For the first-table page's table, whether the
  pre-repair reader produced amounts-only rows and the repaired one did not, so 2548's
  change is attributed or confirmed unattributed.
- A mechanism absent from E is fixed in the trace. The verdict rules are not relaxed.

---

## Verification of the revision

1. **Real fixtures for each rule:**
   - R1-1: 2352 (營業比重 plus 主要銷貨客戶) and 7610 (營業比重 plus 銷售地區), captured with
     captions, and one quantity/value header (5211).
   - R1-2: 8150 or 1709 (部門 in the caption), and 2601 (個體, which must still refuse).
   - R1-3: 6921 (a placeholder row).
   - Negative controls: 6496 (two real axes, still refused) and one financial holding (still
     refused).
2. Tests are plain asserts, never `xfail`; the existing 832 stay green.
3. **Differential run again,** unchanged in design, thresholds (300 / 20 / 65) and baseline.
   K remains the 20 prior rulings; the revision's fixtures are tests, not K entries, because no
   prior ruling names their direction. The run is compared against the frozen v2 baseline,
   **not** against the stopped run.
4. The stopped run's records stay where they are (`tmp/rejudge/p22r/`), and the new run writes to
   `tmp/rejudge/p22r2/`, so both remain auditable.

## Decisions to rule

| id | question | recommended |
|---|---|---|
| R1-1 | disqualify non-revenue tables by caption and header words, producing neither candidate nor refusal | yes, disqualification only; no caption requirement |
| R1-2 | table-level part scope only on 個體 / 母公司 | yes |
| R1-3 | placeholders occupy a value position; pairs decided by form; undecidable pairs refuse | yes |
| R1-4 | single named period on a line header | no rule until 1325 / 1522 / 6547 are read |
| R1-5 | E counts a none-owner with candidates; amounts-only fallback traced | yes |
| — | rerun under the same design into a new directory, compared with v2 | yes |

---

## Implementation notes (2026-09-14)

Patch scripts under `tmp/rejudge/p22r/`: `_patch_r1_1_2.py`, `_patch_r1_1b.py`, `_patch_r1_1c.py`,
`_patch_r1_3.py`. Parser backup before revision 1: `tmp/rejudge/annual_report_revenue.py.bak-r1`.

**R1-1.** Two predicates, each only disqualifying:
- `_non_revenue_heading(heading)`: the heading is the last caption line above the table with any
  unit note removed (`_table_heading`). Word list as designed, matched as a regex so that
  `銷售.{0,6}地區` / `提供.{0,4}地區` catch 7610's 「主要商品（服務）之銷售（提供）地區」.
  **No exception for 營業比重 / 營收比重 in the heading** — the first draft had one, and 5410's
  market-share table (「市場占有率擬以本公司營收占同業之合計營收比重表示」) passed through it.
  Consequence to note: 6496's 「依銷售對象通路別營業比重」 is now excluded by 銷售對象; 6496 still
  refuses on its two product axes.
- `_non_revenue_table(table)`: the header names a non-revenue measure or a 量 | 值 pair, and no
  revenue-share column remains.
- An excluded table yields neither candidate nor refusal. **Amounts-only slot:** an excluded
  table on the first table page closes the slot (`amounts_only = []`) instead of handing it to
  the next table: after 5211's 銷售量值 table was excluded, its 員工人數 | 平均年歲 table became the
  mix (員工人數 10, 平均年歲 44.69). A first draft restricted the fallback to the first table
  object; that would also have dropped an amounts-only mix printed second on its page, so it was
  replaced by this narrower rule.

Real-filing check: 2352, 7610, 5410, 8150, 1709, 2538 read; 2601 D2, 6496 and 2891 still
refuse; 3633, 5452, 2355, 5211, 1338 no rows; 2013, 3432, 6803, 2332, 6462 unchanged.

**R1-2.** `_CAPTION_PART = ("個體", "母公司")`.

**R1-3 — replaced by R1-3′ (ruled 2026-09-14, `db/P22_LINE_CONTRACTS_DRAFT.md`).** Its
form-decided pairs made 2641's amount 30 a 30% row and refused 6856 and 7834; a placeholder is now
a NULL position measured by the header (L2), and `_placeholder_groups` is removed. As first
implemented: `_line_values` lists value positions (a placeholder is `None`); `_placeholder_groups`
aligns them only on a line that holds a placeholder and whose header names periods. Positions =
groups: all shares. Positions = 2 × groups: pairs, each value's form decides its measure, two
shares or two amounts in a pair refuse. A NULL target share skips the row. A placeholder the label
pattern swallowed (「新產品 - 1,234 5%」) is moved back into the values. Lines without a
placeholder are read exactly as before.

**R1-5.** The rerun's scripts are copies in `tmp/rejudge/p22r2/` (the stopped run's scripts are
untouched). `acted()` adds: `_own_section` outcome none with ≥1 candidate; an R1-1 exclusion of
a table the repaired reader would otherwise have read or refused on (counterfactual computed with
the exclusion bypassed, trace sets restored); any R1-3 placeholder alignment; and a first-table-page
amounts-only difference between the pre-repair and repaired table readers. The refusal map gains
`R1-3 placeholder alignment`. Smoke: 2548 is now attributed (page 56, pre-repair amounts-only,
repaired none); 1338 counts through the none-owner rule.

Tests: `tests/test_scope_revision_1.py`; fixtures appended to `tests/fixtures/mix_sections.json`
(16 entries with caption and heading).
