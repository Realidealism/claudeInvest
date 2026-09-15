# P22 repair design — selection by scope identity, not by position

**Status:** ruled 2026-09-14 (D1–D6 below). No code has been written yet. Implementation follows
the order in D6, with each step's fixtures green before the next.

P22 closed as a write-set gate: the 411 carry no confirmed scope-selection error. The reader
defects that produced those errors are still in the code. This document fixes, for each
confirmed shape, the semantic contract the repair must satisfy **before** any code exists.
For each shape it records the current selection predicate, why it fails, the new predicate,
what happens when the predicate cannot be satisfied, which correct reads could change, the
affected set, and how regressions are caught.

Evidence for every claim is in `db/PARSER_BACKLOG.md` (P22) and `tmp/rejudge/m5/`.

---

## 1. The shared contract

The four shapes are one failure: **a value column is chosen before its scope is known, and a
position stands in for the missing knowledge.** So the repair is one contract applied in
four places, not four patches.

### 1.1 Scope identity

Every value the reader might take — a table column, or a token position on a text line —
gets a scope identity with three parts. A part is either established from the filing's own
labels, or `unknown`.

| part | values | established from |
|---|---|---|
| **period** | an ROC or AD year (114 ≡ 2025), `other period`, or `unknown` | header text over the value, including spanning cells: Arabic `114年度`/`2025年`, Chinese-numeral `一一四年度`, `民國一一四年度`, `114度`. A label naming a quarter or a 同期 comparison is `other period` |
| **entity** | `consolidated` · `parent-only` · `segment` · `region` · `unknown` | header cells and table-level caption: 合併 / 全球 / 集團 / 合計 (consolidated or total); 個體 / 本公司 / 母公司 / 單一 (parent-only); 部門 / 館別 / named business units (segment); 內銷 / 外銷 (region) |
| **measure** | `amount` · `share` · `change` · `unknown` | the existing Gate 2 words (`_header_kind`), unchanged |

**Target scope:** period = fiscal year, and entity = consolidated. Consolidated is the P20
ruling: an industry mix describes the group. A `segment` or `region` column is never the
target, but a total column over segments (合計) is consolidated.

**Ruled (D2):** a filing that prints only a parent-only mix does not provide the target scope.
That is a source-availability fact, and the parser does not turn it into a semantic
substitution by storing parent-only evidence as the company's mix. The outcome is
`scope_unresolved`, with the reason `target consolidated scope unavailable`. No schema
change.

### 1.1a Implementation notes from step 0 (`_periods`, `_column_scopes`)

Written while building the primitive against the real fixtures. Each one either keeps a
currently correct read correct or records what a label does and does not establish.

- **本公司 is a weak entity word.** 5487's 佔本公司營收比重 means the company itself. It is
  recorded as `parent_weak` and read as parent-only **only when a consolidated column
  stands beside it** (2332's 本公司 | 合併公司). A label reading 本公司 alone does not
  establish parent-only, so it does not trigger D2 on its own.
- **A share of something other than revenue is not a candidate.** 5487's second column,
  佔台灣IC設計業產值比重, is a share of industry output. A share whose label names 產值,
  市場, 市佔, 佔有率, 進貨, 採購, 產量, 銷量 or 數量 is marked `share_of = other`.
- **Header alignment.** A header row with fewer cells than there are data columns is a
  spanning title covering the columns to its right. Otherwise each data column takes its
  nearest cell, because pdfplumber prints 5310's headers one column off their values.
- **A column's own cell gives its measure before a spanning cell does.** 7883 prints 比重(%)
  one row too high, where it spans onto the 114 amount column.
- **A share inherits missing period or entity from the column immediately to its left,
  unless that column is itself a share.** 8996 prints 個體財報 over the amount, and 2332
  prints 114年度 only over the amounts. 5487's two shares must not lend each other scope.
- **Scope-local amount words.** 營業額, 銷值 and 銷貨 name an amount (4995). They are added
  for scope identity only; `_header_kind` is shared with the current reader and is
  unchanged in step 0.
- **Two tables on one page are identical at column level** (2013). Table-level scope is the
  only thing that can separate them, which the test suite now pins.

### 1.2 Selection predicate

Given the candidates of one measure:

1. **Exactly one** candidate matches the target on every *established* part → select it.
2. **None is labelled in any part** (no period and no entity words anywhere over the values)
   **and exactly one** candidate exists → select it. This is today's single-column table
   (1305, 1515), and it stays accepted.
3. **Anything else** — two or more candidates remain, or the only candidates carry an
   established non-target part → **refuse**. The reader does not pick by position.

Position may still order candidates for display or diagnostics. It may never decide between
them.

### 1.3 The amount follows the share

The amount and the share of one row are taken from **the same scope identity**. The amount
is selected by the same predicate, restricted to the share's period and entity.

**Ruled (D4):** when no amount column has that identity, revenue is `NULL` and the share is
kept. The row is not refused, because the share is legitimate evidence on its own. What is
stored says "the share of this item is known, and its same-scope revenue is not". Revenue is
never borrowed from another period, another entity, another column, or the most similar
amount by position.

### 1.4 Refusal is an outcome, not a fall-through

Today a table the reader declines hands the section to the next reader: the line reader,
then prose. That is how 2404 was re-guessed after the table path correctly refused it.

**Ruled (D1):** a scope refusal is the status `scope_unresolved`, and it is **terminal**. The
reader could not select a candidate uniquely from established period, entity and measure, so
it refuses. The section is not handed to any other reader. The detail carries the header
text, or the caption, that caused the refusal. It is never `parse_conflict`, which asserts
that conflicting evidence was parsed, and on which `save()` retracts (P13).

### 1.5 No silent fallback (ruled as the repair's general rule)

Once a semantic attribute is found to have candidates but cannot be established, it is not
completed from a column position, a token position, first or last, or another reader's
result.

    unknown ≠ first column          unknown ≠ first amount
    unknown ≠ consolidated          unknown ≠ current year

This is the common root of P22-1 to P22-4. Every code path the repair touches is reviewed
against it.

---

## 2. Shape by shape

### P22-1 · unparsed period label

**Current predicate.** `_year_span` (`scrapers/annual_report_revenue.py:681`) and `_year_slot`
(`:1427`) recognise only `(?<!\d)(1\d{2}|20\d{2})(?!\d)`. When no span is found,
`_header_columns` falls back to the first ratio-labelled column (`:751`), and `_year_slot`
returns slot 0.

**Why it selects wrong.** 一一三年度 | 一一四年度 is two periods. Unparsed, it looks like none,
and the fallback treats the first column as the fiscal year. Filings usually print the older
year first. Confirmed: 2404, 2727, 7742. 1268, 1470, 4995, 6020 and 8923 were right only
because the target happened to come first.

**New predicate.** One period parser (§1.1) used by both readers. The fallback survives only
under §1.2 rule 2: no period labelled anywhere and exactly one share candidate. A header that
names periods, none of them the target (7907: 112 | 113), is refused.

**Correct reads that could change.** The positional-only companies change *reason* but not
value. A table whose header year is written in a form still not recognised (1460's spaced
`1 1 4年`) moves from a guess to a refusal. That is a loss of rows, and it is the intended one.

**Affected set.** P22-A strict candidates (30), plus every company whose v2 read used the
`_year_span is None` fallback or `_year_slot` = 0 with two or more shares on a line.

### P22-2 · several scopes under one period

**Current predicate.** Inside the year span, `named()` returns the first matching header cell
(`:770`, `return hits[0]`).

**Why it selects wrong.** 本公司 | 合併公司 both carry 114, so the span holds both, and the first
wins. Confirmed: 2332, 8472, 9930, 1110, 8996, 2492, 4722, 2101.

**New predicate.** Each candidate column in the span gets its entity (§1.1). Select the
consolidated one. Where the span holds two candidates and neither carries an entity word, the
predicate refuses (§1.2 rule 3). Where one column is labelled for a period and the other is
unlabelled, the unlabelled one is not assumed to be consolidated.

**Correct reads that could change.** 8112 (lines, took 合併 by position) keeps its value, with
a reason. A table where the consolidated column is labelled with a word not in the list moves
to a refusal, and the refusal is recorded with the header text so the list can be extended.

**Affected set.** P22-B `ambiguous` (58), and P22-C `pct part scope` / `amount part scope`
findings.

### P22-3 · two tables, one per scope, on one page

**Current predicate.** `parse_pdf` returns the **first** table that yields rows (`:1520`).
Table order is page order.

**Why it selects wrong.** 2013 prints 中鋼結構公司－個體 and then 中鋼結構公司－合併, each with a
single value column. No column-level rule can tell them apart, because each table on its own
is unambiguous. Confirmed: 2013. 3432 and 6803 were right because the consolidated table came
first.

**New predicate — table-level scope ownership.** Scope is decided at two levels, and the
table level comes first:

1. **Collect** every table in the section that yields a mix. This replaces first-wins.
2. Give each table a **table scope** (period, entity) from its caption, plus any scope words
   in the table's own header. **Ruled (D3):** the caption is **table-local**: the text
   between the table's top edge and the nearest heading or previous table above it on the
   same page. Page text is never scope evidence, because a 合併 written over another table
   on the same page must not assign scope to this one. The derivation always runs
   table → its own context → its scope, never page → a word → every table on it.
3. If one table matches the target, it owns the section and Gate 2 runs inside it. If several
   tables match, or none do while tables with different scopes exist, **refuse**.
4. A single mix table with no scope words at all is accepted, exactly as today (§1.2 rule 2
   at table level).
5. **Ruled 2026-09-14 — rule 2 does not survive a refusal.** When any mix candidate in the
   filing was refused, a table with no period label and a single column cannot be
   accepted on rule 2. It has not established the target scope; it is only unlabelled.
   A later table is accepted only when it completes its own scope identity. Position
   (coming later, being unique) never completes it. P22-1 does not enforce this yet,
   because tables are read in page order and an unlabelled table can be accepted before
   a refused one is seen. It lands with the collection step here, and the whole-population
   differential run happens only after P22-3.

**Not a string shortcut.** "Prefer a table whose caption says 合併" is only step 2's input. The
contract is uniqueness: a table owns the section because it is the only one whose scope
matches, not because it contains a word.

**Correct reads that could change.** Filings that print the same mix twice in one section
(for example a business report and 營運概況) must not become refusals merely because the
mix is repeated.

**Ruled (D5):** two candidates are the same evidence when their **evidence identity** is equal:
the same period, entity and measure after scope identification, the same item set, and the
same values in the same item-to-value correspondence. Equal values alone are not identity:
two business scopes can print equal numbers, and equality must not erase the distinction.
Repetition with equal identity never causes `scope_unresolved`. Candidates with differing
identity follow step 3.

**Affected set.** Unknown for the population. Only the 120-company high-risk subset was
checked whole-filing (3 multi-scope sections). The repair's verification run (§3) measures it.

### P22-4 · the line reader takes revenue from the first amount

**Current predicate.** `_rows_from_lines` takes the share at `picks[slot]` (`:1479`) but the
revenue from `amounts[0]` (`:1483`, `:1487`). The two selections are independent.

**Why it selects wrong.** On 113 amount · 113 share · 114 amount · 114 share, slot 1 gives the
114 share and `amounts[0]` gives the 113 amount. Confirmed: 2736, 3046, 6741. Across the
population 32 companies show the shape, 29 of them `NO_CHANGE`, meaning their stored revenue
already carries the wrong period.

**New predicate — one column map per header, applied to both measures.** It is not "take the
second amount". The header line(s) define an ordered list of column identities, for example
`[(113, amount), (113, share), (114, amount), (114, share)]`, built from the period labels and
the measure words 金額 / 銷售額 / 比率 / % under them. Each data line's numeric tokens are
aligned to that list by position *within the line*. Share and amount are both taken from the
identities whose period is the target.

**When the map cannot be built or does not fit.** If the header names periods but not
measures, the map is built only when every data line has exactly *periods × 2* tokens in
amount/share order (amounts over 100, shares 0–100). A line whose token count does not match
the map is not read. A 3046-style unparseable token (「99,65」) breaks the count, so the line
is refused rather than shifted onto 113.

**Correct reads that could change.** Single-period lines (`amount share`) are unaffected. Lines
listing several items (P23) are out of scope and must not be altered by this repair.

**Affected set.** The P22-C line findings (32).

### P22-1 implementation notes (step 1, fixtures green, 804 tests)

- **Table path.** `_period_selection` gives each table one of three outcomes: `target`
  (the columns whose labels name the fiscal year), `unlabelled` (no column names a period)
  or `no_target`. `_header_columns` searches only the target columns. It accepts a header
  printed one column to the right of its values (5310, 2637), and it never searches a span
  running across other years. `_year_span` is removed.
- **Unlabelled tables** are accepted only with exactly one revenue-share candidate (1326,
  1305). With several, no column is chosen.
- **Ties.** Gate 3's `locate` no longer returns the first of equally scored columns. 2739's
  four hotels all sum to 100 under 114, so it now returns nothing.
- **Refusal is raised only for a mix candidate.** It fires after the region, context and
  axis gates, and only when `_looks_like_mix` finds two or more item rows carrying values.
  A research-expense or attendance table on the same page cannot end the reading.
- **Line path.** `_line_periods` replaces `_year_slot`, which is removed. When a header line
  names two or more periods, the shares on each line must be exactly one per period —
  all %-marked shares, or, when unmarked, every value up to 100 — and are aligned in the
  header's order. Otherwise the line refuses. This replaces "the trailing two", which slid
  3046's 113 share onto 114. With no period named, two shares with no item name between
  them refuse (4116). Two items on one line (1762, P23) do not.
- **How terminal a refusal is — an interpretation to confirm.** Inside `parse_pdf`, a
  refused table does not stop the other tables in the filing from being read. A later
  table that establishes the fiscal year's scope on its own labels is accepted, because
  that is not a guess. When no table is accepted and at least one refused, the refusal is
  raised, and the line reader, prose reader and amounts-only fallback never run. A
  line-reader refusal likewise stops prose.
- **Status.** `process()` records `scope_unresolved` with the reason and header text.
  `save()` needs no change: it retracts only on `no_section` and `parse_conflict`, and
  replaces rows only on `ok`. The daily sweep does not retry it.

### P22-2 implementation notes (step 2, fixtures green, 815 tests)

- **`_select_columns`** now returns `(amount_col, pct_col, refusal)`. `_header_columns`
  and `_period_refusal` are views of it, so a column choice and its refusal reason can
  never be computed two different ways.
- **`_entity_select(pool)`**, in this order:
  1. exactly one column that is consolidated or total, and not also part-scoped → it.
     Two or more → refuse.
  2. every column strong parent-only (個體 / 母公司 / 單一 / 個別) → refuse with
     `target consolidated scope unavailable` (D2).
  3. any segment or region column with no total → refuse.
  4. a single remaining column → it. This covers 本公司 alone (5487), which is the
     company itself.
  5. otherwise → refuse, because no period or entity tells the columns apart.
  本公司 is never treated as whole. Beside a consolidated column it simply loses (2332),
  which is the "weak marker upgraded by contrast" rule.
- **Where it runs.** Under the target period, only when the share candidates carry any
  entity word. The table's columns are then narrowed to the chosen entity, amounts
  included. With no period labelled, it runs directly on the revenue-share candidates.
- **The amount follows the entity.** 2332 and 8472 now take the consolidated amount.
  8996's 合併財報 column names no measure, so no amount is taken and revenue is NULL
  (D4).
- **A tightened alignment rule, which a P22-1 latent bug required.** A header cell
  may sit one column right of its values only over a column that carries no values of
  its own. Without this, 9930's 占銷售額% heading the 113 parent-only block was taken as
  the header of the 114 consolidated column once selection narrowed to it.
- **Line reader.** `_line_entity_slot` handles two shares of one period with no item
  between them. A header line naming both a whole-company word and a strong part word
  gives the entity order, with repeats collapsed (8112: 合併 合併營收 個體 個體營收 →
  [consolidated, parent]). The order must account for every share and contain exactly
  one whole entry. Otherwise the line refuses as before. 本公司 is ignored there, so
  narrative text does not supply an order.
- **Sentinels turned.** The 2332 test that asserted the wrong column by name
  (`test_table_gates.py`) now asserts (3, 4) and 39.32. The P22-1 interim refusal of
  4722 is removed, and 4722 now selects 合併.
- **Not fixture-covered:** D2 on a real parent-only-only table. None was captured, and
  fixtures are never typed. The differential run will surface real cases.

### P22-4 implementation notes (step 3, fixtures green, 822 tests)

- **One mechanism on the line reader.** The header declares the **groups** — one per
  period (`_line_periods`) or one per entity (`_line_entity_slot`) — and which of them
  is the target (fiscal year, consolidated). A group's identity has two measures, share
  and amount. Each value's measure is its own form: %-marked, or without marks, up to
  100 is a share and above 100 is an amount. The i-th share and the i-th amount belong
  to group i, and share and revenue are both taken from the target group.
- **Counts.** The shares must number exactly the groups, or the line refuses (as in P22-1;
  3046). When the amounts number exactly the groups, revenue is the target group's
  amount. Any other count leaves revenue NULL and keeps the share (D4); nothing is
  borrowed.
- **One group** (no period or entity declared): revenue is the line's amount only when
  it carries exactly one. The old "first amount" rule survives only on P23's shape (two
  items on one line), which is out of scope and left as it was.
- **2736, 6741, 8112 are one mechanism, not three patches**; the tests pair revenue with
  share so a right share beside another period's revenue cannot pass. 3046 stays refused.
  1268, 2404, 8923 and 1762 are unchanged.
- **5310 needs no line-reader change.** Its table amount was already restricted to the
  fiscal year's own columns by P22-1, and a test pins that under P22-4.
- **Not fixture-covered:** a line whose shares align while its amounts do not
  (revenue NULL). Left to the differential run.

### P22-3 implementation notes (step 4, fixtures green, 832 tests)

- **Collection.** `parse_pdf` now uses `page.find_tables()` and extracts each table with
  `.extract()`. On all five pages checked this is identical to `extract_tables()`, and it
  also gives each table's bounding box. Every table yielding shares, transposed tables
  included, becomes a `_TableCandidate(rows, table, caption)`. Refusals are collected
  beside them, and nothing is returned until `_own_section` decides.
- **Caption (`_table_caption`, D3).** The text from the table's top edge up to the bottom of
  the nearest table ending above it, or the page top, keeping the last three lines. Only
  non-overlapping tables count: 6803's page is wrapped in a text frame that pdfplumber
  returns as a table, and using its bottom would lose 「A. 崑鼎公司及子公司：合併營業比重」.
- **Caption entity words differ from column words, on purpose.** Whole: 合併 / 全球 / 集團.
  Part: 個體 / 母公司 / 部門 / 內銷 / 外銷. 合計 is excluded, because the caption region can
  carry the previous table's 合計 row (3432). 個別 is excluded, because in running text it
  means "respectively" (6803's 個別說明經營概況).
- **`_table_scope`** gives `(established, entity, other_period)`. A table is established when
  its own header targets the fiscal year, its caption names the fiscal year, or it is
  consolidated by its caption or by its selected column.
- **`_own_section`**, as ruled:
  1. When any table is consolidated, only consolidated tables form the pool. Otherwise the
     pool is the established tables that are not part-scoped.
  2. The pool is grouped by evidence identity (`_evidence_groups`: the same items carrying
     the same shares). One group owns the section; two or more refuse.
  3. With no pool, any refusal is raised. This is the rule-2 boundary.
  4. With no refusal either, unlabelled tables are accepted only as a single group.
  5. When only part-scoped tables remain, the section refuses with `target consolidated
     scope unavailable` (D2).
  Within one group, an item whose revenue differs between members gets revenue NULL
  instead of the first table's figure.
- **Real filings through `parse_pdf`:** 2013 → 合併 93; 3432 → consolidated (its p4 and p41
  tables are one piece of evidence); 6803 → A; 2332 → 合併 39.32; 2404 → 114 by the table
  path. 7907, 4116 and 3046 refuse. 6462 and 1305 are unchanged. Parse time is 2–10 s per
  filing, because every section page is read.
- **Two tests pair real tables from two filings**: two different consolidated evidences,
  and a refused table beside an unlabelled single-column table. No cached filing carries
  either shape alone; the tables are captured and only the pairing is test logic.
- The dead post-table `raise refusal` in `parse_pdf` is removed. `_own_section` raises in
  its place.

### Also repaired under the same contract (not P22-1..4)

- **Table amount period (5310).** The header sits one column off its data; `locate("amount")`
  searches the whole span and lands on 113年同期. Under §1.3 the amount must share the share
  column's identity, so a relocated amount is accepted only when its own header identity
  matches.
- **Business scope (2739, 4131).** Per-hotel and per-segment columns are `segment`. A table
  whose only value columns are segments, with no total, is refused.

---

## 3. Verification — how a repair is shown not to break another shape

1. **Fixtures from real PDFs, never hand-transcribed** (existing rule). Positive:
   2404 and 2727 (P22-1); 2332, 8472 and 9930 (P22-2); 2013 (P22-3); 2736, 6741 and 3046
   (P22-4); 5310; 2739. Negative, currently correct and required to stay unchanged:
   6462, 2637, 4995, 1470, 1305, 1515, 3432, 6803, 8112, 1268. Every assertion is a plain
   assert naming the defect, not `xfail`.
2. **The existing suite (761) stays green.**
3. **Whole-population differential run.** Re-read all 2302 cached filings with the repaired
   reader and compare each company to `full_candidate_v2.jsonl`, with closure asserted.
   - Every company whose rows change must belong to the **expected-change set**: the union of
     the P22-A strict candidates, P22-B ambiguous, P22-C findings and the 120 whole-filing
     candidates, together with the refusals the contract predicts.
   - **A change outside that set is a regression until read at the source.** It is not
     treated as a bonus fix.
   - Refusals are counted and listed by shape. A refusal count far above the affected set
     means the predicate is too strict.
4. **Then, in order:** re-read → reconciliation → ownership → new write set → M5 → M6 → M7.
   411 is the starting point of that recomputation, not its answer.

---

## 4. Decisions (ruled 2026-09-14)

| id | ruling |
|---|---|
| **D1** | `scope_unresolved`: the reader cannot uniquely select from established period, entity and measure, so it refuses. **Terminal:** the section is not handed to another reader. The detail carries the triggering header text. Never `parse_conflict` (§1.4) |
| **D2** | Parent-only mix only → `scope_unresolved`, reason `target consolidated scope unavailable`. Not stored as parent-only evidence; no schema change (§1.1) |
| **D3** | Caption is table-local: from the table's top edge up to the nearest heading or previous table on the same page. Page text is not scope evidence (§2, P22-3) |
| **D4** | No same-scope amount → revenue `NULL`, share kept, row not refused (§1.3) |
| **D5** | Repeated candidates with equal evidence identity (period, entity, measure, item set, item-to-value correspondence) are one piece of evidence and never cause a refusal. Equal values alone are not identity (§2, P22-3) |
| **D6** | Order: shared scope identity (§1) → P22-1 → P22-2 → P22-4 → P22-3 → one whole-population differential run. Each step's fixtures green before the next. A difference outside the expected-change set is a regression until read at the source, however much more plausible the new value looks |

**Standing rule for the implementation:** §1.5, no silent fallback.

## 5. Out of scope for this repair

P21, P23 (several items on one line), P24 (statute number read as a share), P25 (market-share
table), P26 (swapped share/amount labels), P13, P14, P17 and P18. Where a change here would alter one
of those shapes' outputs, the differential run catches it, and it is reported rather than
silently kept.
