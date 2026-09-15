# Parser backlog — annual-report revenue extraction

What `scrapers/annual_report_revenue.py` is known not to read, known to read
wrongly, or reads on a basis that has not been justified. Written 2026-09-11,
when the list existed only in conversation.

This is not a to-do list of ideas. Every row below is a shape or a defect that
was **observed in a filing**, with the company that showed it and the evidence
that would close it. A row with no observed population does not belong here —
that rule is the same one `TAXONOMY_GAPS.md` keeps, and for the same reason: a
speculative entry costs nothing to write and then justifies work nobody
measured.

## How a row leaves this file

| status | meaning |
|---|---|
| `open` | observed, reproducible, nobody has worked it |
| `measuring` | a controlled comparison is running that will size or close it |
| `absorbed` | a later change appears to have fixed it; the re-measure that would confirm has not run |
| `ruling` | not a parser question — it needs an adjudication (R019/R023/R025) |

Anything that changes the reader must be measured the way the batches before it
were: same PDF, one rule varying, every company in the population getting
exactly one recorded outcome, `measurement_error` kept out of the reader's
score (`.claude/rules/02_judgment.md` Rubric 8).

---

## P1 · amounts-only fallback: does it have a population at all?

**status** `measuring`

`parse_pdf` falls back to reading amounts when no ratio column is found, and
`tw.product_revenue_share` then derives shares from those amounts. Before the
header fix, 12 companies were served this way and **all 12 derived to exactly
100.00** — necessarily, because the derivation divides the amounts by their own
sum.

Audited one by one on 2026-09-11, after the header fix:

| outcome | companies |
|---|---|
| now read the disclosed ratio, fallback not needed | 8 — 1584 精剛, 2637 慧洋-KY, 3379 彬台, 3473 智通聯網, 5262 立達, 6231 系微, 6862 三集瑞-KY, 3633 云光 |
| still on the fallback | 3 — 2438 翔耀, 2545 皇翔, 4726 永昕 |
| reads nothing now | 1 — 2883 凱基金 (see P3) |

None of the remaining three is a case the fallback serves correctly: 2438 is a
different table shape (P2), 2545's table is by sales region (P5), and 4726's
page holds a cash-flow variance table and no mix at all (P6).

**The finding worth keeping**: a fallback that derives shares from amounts
cannot use "the numbers add to 100" as evidence that it read a mix, because
that sum is arithmetically guaranteed. Its success condition coincides exactly
with upstream extraction failure. 4726 永昕 is the clearest proof — there is no
mix on that page and the fallback produced a 100% distribution from a cash-flow
table.

**closes when** the 1687-company table-path comparison finishes and shows
whether any company newly falls into the fallback. If none does, the path
should be retired rather than extended.

---

## P2 · a mix table with no amount column

**status** `open` · observed: 2438 翔耀

```
項目/年度                            113    114
光學面板、彩色濾光片基板等加工及產銷    89%    72%
Others                              11%    28%
```

Two ratio columns, one per year, and no amounts anywhere. The header names only
the years, so the ratio vocabulary finds nothing and `_header_columns` returns
the year column as an amount. The values are the percentages themselves.

**closes when** a reader recognises "the year column holds ratios" — which the
data can attest (0..100, summing to ~100) once the header has said which year.

---

## P3 · aggregate captions the vocabulary does not know

**status** `open` · observed: 2883 凱基金 (「總保費收入」), and earlier 1338 廣華

`AGGREGATE_ROW` holds 合計 / 總計 / 小計 / 總額 and the net-sales wordings. An
insurer writes its total as **總保費收入**, which contains none of them.

Consequence is not a wrong row but a lost company: with the ratio column now
read, 2883's rows sum to about 200 and the 50..150 completeness bar correctly
rejects the whole read. Before the header fix the same table passed, because
every pct was NULL and the sum was 0.

**closes when** the aggregate vocabulary covers industry-specific totals, with
the blast radius measured against stored rows first — the last extension of
this vocabulary was measured and rejected nothing, and that is the bar.

---

## P4 · anonymised counterparties written with Latin letters

**status** `open` · observed: 3633 云光 (「A公司 65.7 / 其他 34.3」)

`_ANON_PARTY` matches 甲/乙/丙… 公司 — the heavenly stems — because that is what
3709 鑫聯大投控's major-customer table used. 3633's uses `A公司`. Same table
type, same failure, different alphabet.

**closes when** the guard covers single-letter aliases. Keep it whole-label: a
product may legitimately begin with a Latin letter.

---

## P5 · a ratio table that is not a product mix

**status** `ruling` · observed: 2545 皇翔 (「年度銷售地區 … 金額 比率」), 3633 云光

The header itself names the axis — 銷售地區 — and the reader has no test that
asks what the rows are *of*. Reading it produces 台北市可售金額 / 新北市可售金額
as revenue lines.

This is not a table-parsing defect; the table parses correctly. What is missing
is a check on the axis, and that check has to be written as a rule, not as a
word list, or it becomes the blacklist this parser keeps failing with.

---

## P6 · pages with no mix table where the reader finds one anyway

**status** `open` · observed: 4726 永昕

The mix page filter matches any page carrying 營業比重 / 主要商品 / 營業比率 /
營收比重, and those words appear in prose too — 4726's match is a sentence about
the change in service revenue, on a page whose tables are the cash-flow
statement. The `amounts_only` gate added for 1338 廣華 (first table-bearing page
only) does not help when the *first* table on the matching page is the wrong one.

---

## P7 · shapes the no_section taxonomy named and nobody has read

**status** `absorbed`? for T2, `open` for the rest — all need re-measuring after
the header fix

| shape | companies | note |
|---|---|---|
| T2 · ratio column with no % sign | 8 | likely absorbed: 1584 精剛 (83) and 6862 三集瑞 (57.66) now read. Re-measure the no_section population before closing |
| T4 · two columns, label + %, no amount | 10 | overlaps P2 |
| T3 · transposed in plain text | 2 | `_rows_from_transposed` reads `extract_tables()` output; 1104 環泥's transposition is in the text layer, not in a table |

---

## P8 · prose input is sliced before the first label

**status** `open` · observed: 1313 聯成 (loses 酸酐類 7%)

The prose pass reads `text[text.find("營業比重"):]`. When the first occurrence of
the heading is *inside* the first data clause — 「其中酸酐類營業比重為7%」 — the
label is on the wrong side of the cut and only the later products are read. The
company is stored at 88% instead of 95%.

Changing the slice affects every prose reader at once, which is why it was not
done inside the reader batch that found it.

---

## P9 · merged-cell hierarchies

**status** `open` · observed: 1338 廣華-KY

A parent label spans four sub-rows and is filled only on the first of them, so
the reader takes column 0 and skips three real products. Fixture and the full
explanation are in `tests/test_revenue_aggregate_rows.py`.

---

## P10 · a branch office is a line in the company's own table

**status** `ruling` · observed: 3015 全漢 (「高雄分公司 2.30%」)

The filing's own mix table lists it. The parser read it faithfully. Whether a
branch is a revenue line for classification purposes is a question for the
ruleset, not the reader.

---

## P11 · the retract predicate cannot tell one producer from another

**status** `fixed 2026-09-13` · scoped to `READER_METHODS` in save(), pinned by
tests/test_revenue_row_lifecycle.py · observed on all 141 companies holding
narrative rows

`save()` retracts when the status is `no_section` or `parse_conflict` and the
stored row's `source_file` is NULL or matches `2025_<sid>_%`. Neither clause asks
who wrote the row. Measured across FY2025:

| method | live rows | caught by the predicate |
|---|---|---|
| `narrative_table` | 250 | 250 — all, via the LIKE pattern |
| `narrative_text` | 234 | 234 — all, via `source_file IS NULL` |

Those 484 rows belong to `annual_report_narrative_tables.py` and
`annual_report_narrative_text.py`, which insert their own method literal
directly. `parse_pdf` cannot return either method, so a company whose reader is
silent would have *another producer's* rows retracted by a reader that never
owned them. 1103 and 1201 show the two halves: reader silent, and reader
overriding.

The predicate answers "does this row look like the thing I am retracting?" when
the question it has to answer is "is this row mine?".

**closes when** retraction is authorised by producer ownership rather than by
source_file shape — a reader apply may only update or retract rows it produced.
Ruled 2026-09-12 as a hard gate on the apply step. The candidate snapshot is not
affected: it writes nothing, which is exactly why the danger sits in apply.

---

## P12 · the success-path DELETE has no producer or provenance condition

**status** `fixed 2026-09-13` · the DELETE is now scoped to `READER_METHODS` and
spares `retracted_at IS NOT NULL`, pinned by tests/test_revenue_row_lifecycle.py
· **was more dangerous than P11**

`save()` line 1570, on the *success* path:

```sql
DELETE FROM tw.product_revenue WHERE stock_id = %s AND fiscal_year = %s
```

No method filter. No `source_file` filter. No `retracted_at IS NULL`. Whenever a
company reads `ok`, every row it has for that year is physically removed.

Measured population:

| what the DELETE would take | n |
|---|---|
| companies holding narrative-producer rows, **all of them at status `ok`** | 141 |
| retracted rows across FY2025 — the audit trail migration 112 exists to keep | 57 |

P11 is about the retract predicate on the *failure* path, and retraction is
reversible: the rows stay and stop being evidence. This is the opposite on both
counts — it fires on the ordinary success path, and it destroys rather than
retracts. The `reader_would_override` outcomes in the full re-run are exactly
this population arriving.

**closes when** the DELETE is scoped to rows this producer owns and never touches
`retracted_at IS NOT NULL`; superseded rows should be retracted, not deleted.

---

## P13 · one parse conflict retracts everything, and nothing ever retries it

**status** `open` · verified 2026-09-12 · **the mechanism is the defect; these two retractions removed wrong data**

`_dedupe` reporting the same item twice with different values sets
`status = 'parse_conflict'` and `unique = []` (line 1529), which then satisfies
the retract condition at line 1554 — so a single ambiguous row withdraws the
company's entire mix. `parse_conflict` then appears in **neither** retry set:
`daily_sweep` selects `IN ('download_failed','parse_failed')` (1654) and
`--retry-failed` selects `IN ('download_failed','parse_failed','no_section')`
(1743).

What was withdrawn on 2026-09-10, and what the filings actually say. Restoring
these was ruled and then withdrawn, because checking them against the cached PDFs
showed the stored rows were **not** the correct data:

| company | rows withdrawn | what the filing prints | verdict |
|---|---|---|---|
| 5011 富喬 | 9, summing 94.11 | 12 rows in two segments, summing 100.00 | stored state was a lossy partial read |
| 2349 陞技 | 3, summing 104.00 | 2 rows only: 光學產品 86.77 / 其他 13.23 | stored state contained a fabricated row |
| 4114 | 0 live | — | nothing was persisted |
| 2388 威盛 | never had rows | — | conflicting item 約占 is parser noise |

**5011** (page 87) discloses the same item names under two segments — 售電收入
3.22 / 工程收入 2.27 / 操作收入 0.40 in the main list, and 售電收入 5.14 /
工程收入 0.06 / 操作收入 0.57 again under 特許權協議. The stored 9 rows kept the
concession value of each duplicated pair and dropped the main-list one; the
missing 3.22 + 2.27 + 0.40 = 5.89 is exactly the gap between 94.11 and 100.00.

**2349** (page 57) prints two items that already sum to 100.00. The stored `USB
4.00` appears nowhere in the mix table — an older reader lifted it out of the
surrounding product prose.

So the conflict detector was right on the facts both times, and the root cause
under 5011 is that the parser cannot represent a **segment × item** hierarchy and
can only report a collision. The defect this entry records is the *mechanism* —
one collision withdraws the whole company, and no scheduled path ever returns —
not the fate of these two.

**closes when** a conflict is treated as "read nothing trustworthy" — i.e. it
does not retract, matching `parse_failed` — and `parse_conflict` joins both retry
sets. "Drop only the conflicting item" is **not** a fix: on 5011 that would keep
a mix silently short by 5.89 points, which is how the stored state got there.
Representing the segment hierarchy is the real close, and it has its own cost.

---

## P14 · the English-edition guard is on only one branch

**status** `open` · verified 2026-09-12 · observed population: 1

`_pick_report` excludes English editions by filename (`FE\d`) only in the branch
that runs when a filing carries *no* description. A filing described as
「股東會年報(英文版)」 takes the other branch and can win on upload timestamp.
Production holds exactly one such row: 5344, `2025_5344_20260617FE4.pdf`, status
`no_section` — the Chinese reader finds nothing in an English document, and
`no_section` then retracts the company. Whether the current picker or its
predecessor wrote that row is **not established**.

**closes when** both branches apply the same exclusion.

---

## P15 · one accounting negative disables the own-total detector

**status** `fixed 2026-09-13` · `_accounting_num` reads what the filing prints, and
an amount that still cannot be read abandons the inference instead of being
skipped · pinned by `test_an_accounting_negative_does_not_change_a_rows_identity`

`_unnamed_total_row` decides a row is the table's own total by arithmetic: its
amount equals the sum of the others. `_to_num` returns `None` for `(1,485,732)`
and `△x`, so such a row never enters `seen`, the sum no longer matches, and the
detector returns `None`.

Reproduced on the real `2897_income_types` fixture by rewriting one ordinary
row's amount in parenthesised form:

```
_unnamed_total_row   10  ->  None
_rows_from_table     []  ->  ('淨收益', 100.0) emitted as a product, rows sum 125
```

125 sits inside the 50..150 window, and there is no declared total to check
against because the declared total is the row being emitted. That is the 2897
regression exactly as it was before the fix.

This is not an argument for changing the shared `_to_num` — that was measured and
rejected (reader unchanged 88, changed 2, both worse). It is an argument that the
*completeness arithmetic* needs to read what the filing prints. Banks print
negatives this way, and 2897 is a bank.

**Both were done, and the second one is what made it delicate.** Refusing to infer
from an amount that cannot be read is right, but "cannot be read" has to mean a
number that failed to parse -- not a label. Two screens were wrong before the
third was right:

| screen | what it broke |
|---|---|
| bail on any unparseable amount | 2897's header row 0 says `2025年度` -- detector removed outright |
| also skip row 0 | 2897's header is **two** rows deep; row 1's amount cell says 金額 |
| also skip cells matching `_YEAR_CELL` | ROC years are three digits, so `100` is both a year pattern and an ordinary amount -- real amounts vanished from the sum |

What holds: skip row 0, treat a cell with no digit at all as claiming no amount,
and abandon the inference only when a cell that does carry digits cannot be read.

---

## P16 · with no percentage column the own-total guard is vacuous

**status** `fixed 2026-09-13` · with no percentage column the inference is not made
at all · pinned by `test_the_own_total_is_not_inferred_without_percentages`

The percentage guard is `pct is None or 95 <= pct <= 105`. When the table has no
percentage column at all, `pct_col is None`, every row's pct is `None`, and the
guard is satisfied unconditionally. What remains is "at least 4 rows" and "equals
the sum of the rest" — which any real product holding exactly 50% of a table
satisfies by arithmetic.

```
[['項目','金額'], ['甲','500'], ['乙','200'], ['丙','200'], ['丁','100']]
_unnamed_total_row(pct_col=None)  ->  1     # drops 甲, the largest real product
```

It is silent twice over: the row is removed, and because every pct is `None` the
declared-total check cannot fire either. `parse_pdf` then stores the remainder as
`table-amounts` (12 companies are on that method today).

**closes when** `pct_col is None` returns `None` outright — with no percentages
there is no evidence any row *claims* to be the whole, which is what the guard
was for.

---

## P17 · a blank cell in the total row switches the completeness check off

**status** `open` · verified 2026-09-12 by reproduction

`_declared_total` returns `None` both when the filing prints no total and when it
prints one whose percentage cannot be read. The caller cannot tell those apart,
and treats the second as the first. Merged cells and line wraps routinely leave
such a cell empty.

Reproduced on the real `6451_geography` fixture by clearing one cell:

```
合計 row: ['合計','5,188,000','100.00','7,531,930','']
_declared_total   100.0  ->  None
_rows_from_table  []     ->  [('美洲', 50.85), ('其他', 2.06)]
```

`PARSER_TABLE_GATES.md` states that 6451 is caught by this check. One blank cell
is enough to get past it, and a geography table is stored as a product mix.

**closes when** "has a named total whose value cannot be read" is a distinct
answer that makes the caller refuse the table — a total that exists but is
unreadable is evidence of lossy extraction, not evidence of no total.

---

## P18 · the completeness tolerance outgrows a whole row at about 15 rows

**status** `open` · verified 2026-09-12 by arithmetic

Tolerance is `max(2.0, 0.5 * len(out))`; an average row is `100/n`.

| n | tolerance | average row | a whole missing row detectable? |
|---|---|---|---|
| 10 | 5.00 | 10.00 | yes |
| 14 | 7.00 | 7.14 | yes, barely |
| 15 | 7.50 | 6.67 | **no** |
| 20 | 10.00 | 5.00 | no |
| 30 | 15.00 | 3.33 | no |

The 0.5-per-row rationale is integer rounding, which only applies to rows printed
as integers. 2884 玉山金 prints to two decimals — maximum rounding error 0.005 per
row — and still receives the same 0.5.

**closes when** the allowance is derived from how each row is actually printed
(0.5 for integers, 0.01 for decimals) with an absolute ceiling. Checked against
both anchors: 2897 (integers, 3 rows) keeps its 2.0 floor; 2884 tightens to 2.0.

---

## P19 · header detection can consume the whole table

**status** `fixed 2026-09-13` · traced by reproduction, pinned by
`test_a_percentage_only_table_keeps_its_percentages` on the real 3054 table

The loop that decides where the header ends stops at a value above 100, or at two
numbers in one row. A percentage-only table with a **single** year column offers
neither: every data row carries exactly one value, and a percentage is never
above 100. So the loop consumed all four rows of 3054 立萬利's table and left
`body` empty.

Everything after that is downstream of the empty body. Gate 2 names the pct
column correctly; Gate 3 looks for its values, finds none, concludes the header
is contradicted by its own data and drops the claim; the merged-header fallback
then assigns that same column as the amount. The mix is stored with NULL
percentages although the header says 營業比重 in words.

```
row 0  nums=[]        -> header      ['產品別', '主要用途', '114年度營業比重(%)']
row 1  nums=[64.93]   -> header      neither test fires
row 2  nums=[35.07]   -> header
row 3  nums=[100.0]   -> header      head has eaten 4 of 4 rows
_header_columns -> (2, None)         the ratio column handed over as the amount
```

Fixed by refusing to let the header be the entire table: when the loop consumes
every row, the header is row 0 and the rest is data. The change reaches only
tables where the old rule found no data at all, which is exactly the broken case.

Restored, each to precisely what was stored before:

| company | restored to | sum |
|---|---|---|
| 2301 光寶科 | 光電 17 / 資訊及消費性電子 38 / 雲端及物聯網 45 | 100.00 |
| 2426 鼎元 | SI元件 70.4 / 化合物半導體 28.9 / 其他 0.7 | 100.00 |
| 3054 立萬利 | 電子 64.93 / 生技 35.07 | 100.00 |

**4545 銘鈺 is not this defect.** It was grouped here on the shared symptom of a
lost percentage. Its table is merged-cell and sparse — labels in one column,
values scattered to indices 6 and 13, most cells empty — and it still reads 85
after the fix. That belongs with P9.

**An earlier note here claimed this corrected P2's diagnosis. It does not.** That
claim rested on the wrong mechanism. P2's 2438 翔耀 has two ratio columns, one
per year, so its data rows carry two numbers and the header loop stops where it
should. P2 stands as written.

---

## P20 · a parent-only share column is read where the consolidated one is printed

**Subsumed 2026-09-14 into P22 as its consolidation subtype.** The entry stays for its
evidence; the defect is tracked under P22.

**status** `open` · observed 2026-09-14 on the fixed tree · 2332 友訊

The mix table prints the same year twice, once per reporting scope:

```
產品項目        本公司                          合併公司
                114年度銷售金額  占全年度營收比重(%)  114年度銷售金額  占全年度營收比重(%)
交換機產品      2,590,003       50.73               5,256,313        39.32
其他產品          203,149        3.98               5,061,526        37.87
合計            5,105,154      100.00              13,367,019       100.00
```

The reader takes the first share column for the wanted year, which is 本公司. An
industry mix describes the group, so the consolidated column is the right one —
and it is what the store already holds. Found through `HOLD_VALUE_CONFLICT`, where
it was the one case in nine in which the stored value was right and the reader wrong.

**closes when** a table that labels its value columns by scope prefers the
consolidated (合併) column, and a table that offers only the parent's is recorded
as such rather than read as the group.

**Ruled 2026-09-14 as a reader semantic boundary, not a reconciliation action.** A revenue-mix reader has to recognise entity scope first: where one table prints both 合併 and 本公司 columns, only the column matching the scope of the business being described is evidence. Column position, numeric completeness and recency may not decide it alone — both columns sum to 100, carry a plausible item set and come from the newest filing, so arithmetic cannot see the error. Until the reader is fixed, reconciliation rejects its parent-only read of 2332 and keeps the stored consolidated evidence. Fixture: `2332_scope_columns` in tests/fixtures/mix_tables.json.

**Second case, 2026-09-14 (P22 scan): 1110 東泥, and its stored data is already wrong.**
The header is `項目 | 個體: 金額(仟元) 百分比(%) | 合併: 金額(仟元) 百分比(%)`, and the
reader takes 個體 (水泥 82.73, 爐石粉 10.35, 租賃 6.92; 合併 prints 水泥 60.13). The
table has no year, so `_year_span` finds nothing and the 1326 fallback hands over the
first ratio column. The stored rows are that same 個體 read, so reconciliation reports
`NO_CHANGE` and nothing would be written. It is the regression-independence case in
its plainest form: the database not changing is exactly how a wrong store stays
wrong. 2332 was caught only because its stored value happened to be the right column.

---

## P21 · the fixed tree reads a fragment row back in

**status** `open` · observed 2026-09-14 · 9924 福興 · no database effect

The stored mix carries a stray line, `h.美規ANSI Grade 1.0`, beside `門用相關製品
100.0` (sum 101). The pre-fix reader dropped it; the fixed reader reads it again,
identically to what is stored, so reconciliation reports `NO_CHANGE` and nothing
would be written. One of P15, P16 or P19 changed which rows this table yields. It is
recorded so that `NO_CHANGE` does not hide a reader that got worse on a company.

**closes when** the change responsible is identified and the fragment is refused
again, with the stored row then reconciled as a removal.

---

## P22 · non-semantic scope selection

**status** `open` · observed 2026-09-14 in M5 classification evidence (2404 漢唐) ·
**redefined 2026-09-14** after the population scan

**Write-set gate CLOSED 2026-09-14** (see db/RECONCILIATION.md). The defect itself stays
`open` until the repair lands: closing the gate removed the errors from the write set,
it did not fix the reader.

**Definition (ruled).** When the reader cannot establish a column's period, entity,
consolidation or business scope from its header, it lets the column's position act as
the selection authority. Subtypes:

    scope
    ├─ period          一一三年度 | 一一四年度; 112 | 113 with no 114 at all
    ├─ entity
    ├─ consolidation   個體 / 本公司 / 母公司 / 單一  vs  合併 / 全球   (formerly P20)
    ├─ business        生技部門 | 醫療器材部門 | 合計; 內銷 | 外銷 | 合計
    └─ other labelled scope

The root failure is that Gate 2 does not treat scope as a semantic fact, so the reader
falls back to position. It is not a Chinese-numeral bug: that is one form of it.
P20 (2332) is its consolidation subtype, not a separate rule.

**The 2404 mechanism, as first traced.** The table path refuses the table correctly
(`_year_span` finds no Arabic year, the header has no ratio word, `_rows_from_table`
returns nothing), and the line reader then reads it (`method = lines`). `_year_slot`
looks for two Arabic years (`1\d{2}|20\d{2}`), finds none, and returns slot 0, so every
data line yields its first percentage: 99.46 / 0.07 / 0.47 is the 113 column (114
prints 99.54 / 0.04 / 0.42). The table path has a sibling fallback (1326 台化: no year
in the header → the first ratio column is the current year).

**Two scans, because one path cannot see the other's failures**

- **P22-A · fallback-triggered** (`tmp/rejudge/m5/p22_scan.py`, merged by
  `p22_merge.py`): no period recognised, so a default column was taken. Mechanical
  closure PASS: 8/8 shards, 2302 = 2302 unique, one record each, the 65 pre-shard
  companies re-read identically, and the three tiers sum to N. Script output: N = 30 =
  confirmed 8 + positional 8 + undetermined 14, every tier ∩ WRITE = 0. **Semantic
  closure FAIL:** reading the headers found 3 false negatives (2492 母公司個別 vs 全球,
  4722 單一 vs 合併, 4131 生技部門 vs 合計) and 6 false positives (1446 and 3630 took
  「-%」 for a header; 1453 and 8210 print a single year; 8112 is consolidation, not
  period; 5487's second column is an industry-output share, not revenue). Observed
  confirmed mismatches, about 11, none in WRITE: 2404, 2727, 7742 (Chinese-numeral
  period); 7907 (112 | 113, no 114 in the table); 6592 (corrupted header); 1110, 8996,
  2492, 4722, 2101 (consolidation); 4131 (business). **Observed, not exhaustive.**
- **P22-B · ambiguity** (`tmp/rejudge/m5/p22b_scan.py`, merged by `p22b_merge.py`): the
  period WAS recognised, but two or more scope candidates sit under it. This is where
  2332 lives, which P22-A structurally cannot see. Validated on 2332, 1110, 2492, 4722,
  8996 and 8112 (positive) and 6462, 4995, 1305, 2404, 2727, 7742, 1762, 2345 and 3716
  (negative) before the full run.

- **P22-B results, read at the source (2026-09-14).** 58 ambiguous, 9 in the write set.
  Confirmed in the write set: 8472 and 9930 (consolidation), 2739 (business: a per-hotel
  guest mix), and 5310 (period, on the amount column: share 114, amount 113年同期).
  Read and right: 1584, 4197, 5297, 6182, 6609, 7781, 7883, and 34 of 38 write candidates
  whose chosen column fell outside the rebuilt target candidates (label misalignment,
  not reader error). The four are rejected in reconciliation.
- **P22-C · amount-column scope** (`tmp/rejudge/m5/p22c_scan.py`, merged by
  `p22c_merge.py`). A read is scope-safe only when both the share column and the amount
  column carry the target scope. The line reader is checked too: it takes the share at
  `_year_slot` but the revenue from the first amount on the line. The scan also records
  risk features, so the unambiguous write set narrows to a high-risk subset read in full.
  Header labels are aligned to data columns by distance, because pdfplumber can print a
  header one column off its values (5310).

**Stored evidence already wrong** (`NO_CHANGE`, so nothing would be written and nothing
would flag it): 1110, 2727, 7742, 7907, 8996, 2492, 4131. None carries an exposure.

**Confirmed shapes (ruled 2026-09-14 as parser correctness defects, not hypotheses)**

| shape | mechanism | confirmed on |
|---|---|---|
| P22-1 · unparsed period label | `_year_slot` / `_year_span` read only Arabic years; 一一三年度 \| 一一四年度 falls back to position | 2404, 2727, 7742 |
| P22-2 · several scopes under one period | 個體 / 本公司 / 母公司 / 單一 vs 合併 / 全球 columns in one table; the first is taken | 2332, 8472, 9930, 1110, 8996, 2492, 4722 |
| P22-3 · two tables, one per scope, on one page | each table has one value column, so no column-level scan can tell them apart; the first table is taken | 2013 |
| P22-4 · amount from the first amount on the line | the line reader takes the share at the year slot and the revenue from the first amount | 2736, 3046, 6741, plus 29 `NO_CHANGE` companies whose stored revenue already holds the wrong period |
| business scope | per-hotel / per-segment tables read as the company mix | 2739, 4131 |
| period on the amount column (table) | share column 114, amount column 113年同期 | 5310 |

The parser is repaired only after P22 closes, and the order is fixed: repair, then re-read
only the affected shapes and companies, then recompute reconciliation and ownership, then
finalise the write set. A repaired parser is not used to prove that P22 closed.

**closes when** a column is selected only after its period and scope are recognised as
the target. A multi-column table whose target scope cannot be established is refused,
not guessed, whichever path reads it. Closure is measured by both scans, with each
confirmed mismatch read at the source.

---

## P23 · a line listing several items yields only its first

**status** `open` · observed 2026-09-14 in the P22-A scan · not a scope defect, kept apart

「生物科技產品 71%;化學合成產品 26%」 (1762), 「網路應用設備 62%  網路接取設備 1%」
(2345), and 「人用藥品 74%、動物用藥 8%、醫療器材 8%…」 (3716). The line reader takes
one percentage per line, so every item after the first is dropped, and the stored mixes
are incomplete. All three are `NO_CHANGE`.

---

## P24 · a statute number read as a percentage

**status** `open` · observed 2026-09-14 in the P22-A scan · 5533 皇鼎 · `NO_CHANGE`

「新青安貸款排除於銀行法 72-2 條之外」 is stored as an item with 72 percent.

---

## P25 · a market-share and volume table accepted as a mix

**status** `addressed by R1-1` (2026-09-14: a header of 需求量 / 銷售量 / 市場佔有率 is not a mix
candidate) · observed 2026-09-14 reading the write set · 5452 佶優 · rejected in reconciliation

Page 96 prints 產品應用 | 114年需求量 | 114年銷售量 | 市場佔有率. The reader takes the market
share as the mix (shares sum 112, inside the 50–150 completeness bar) and the sales volume
as revenue. The stored rows already carry that volume as revenue. R020 ①-C.

---

## P26 · a header that swaps its share and amount labels

**status** `open` · observed 2026-09-14 reading the write set · 3623 富晶通 · rejected in reconciliation

Page 78 labels 銷售比重 over the amounts and 銷售金額 over the shares. The reader resolves
the amount and share to the same column and writes 99.84 as both revenue and pct. The
share is right by accident. E2-01 Core cites the line.

---

## Downstream, not parser

**3633 云光 — re-adjudication.** Its `E2-02` Core (63.522) and `E2-01` Major
(30.189) are weights derived from amounts. The rows they were computed from
(`LCD模組`, `背光模組`) are **not reproducible by today's parser**, which reads
a customer-concentration table on that page instead. That the old and new
header readers agree is not evidence the stored data is right — they agree on a
table that is not the stored one. Ruled on 2026-09-11 to wait for the full
table-path comparison before re-adjudicating under R023-R.

**Settled 2026-09-14 on the full comparison: the reader is rejected, not the
classification reopened.** Opening the page the v2 reader used showed it is not a
customer table but 主要供應商資料 on page 47 — `環鴻(昆山)` 12.23, `日亞` 8.05,
`州巧(廈門)` 5.86, `其他` 73.86 under 占全年度進貨淨額比率(%). Purchases by supplier
are never company revenue items (R020 ①-C). It is the Gate 1 label-column defect
live, not latent (PARSER_TABLE_GATES.md). Reconciliation rejects the read
(`SOURCE_REJECTED` in tmp/rejudge/reconcile.py); the stored `E2-02` / `E2-01`
evidence stands and 3633 leaves the classification-impact queue. That the stored
rows cannot be reproduced remains true and is a reader question, not a reason to
reclassify.

**2884 玉山金 — an adjudication the parser now contradicts.** Migration 110
retracted its four income-statement captions and set `no_section`, explicitly
pending the amounts-only fallback fix. The full re-run reads those same four
captions through the **table** reader, with real percentages this time
(手續費收入 81.0, 利息收入 13.72, 金融商品損益 5.15, 其他營業淨收入(損失) 0.13),
so the condition 110 was waiting on has changed on this path. That does not
retire 110 by itself. Ruled 2026-09-12: the candidate result is **held** — not
saved, not retracted, status not moved back to `ok` — and 2884 gets its own
migration-110 reconciliation once the full run is in.
`test_an_income_type_table_no_one_else_breaks_down_is_still_read` is a parser
contract about what the reader must be able to read; it is not an assertion
about the company's DB status and must not be read as one.

**The nightly sweep runs the working tree.** `InvestAnnualReportSweep` executes
`python -m scrapers.annual_report_revenue --daily` from `C:\Claude\Invest` at
03:00 daily, on system Python — not from `daily_update.exe`. Uncommitted parser
changes are therefore live the following night. The exposure is bounded, since
the sweep selects only companies with no status row, `download_failed`,
`parse_failed`, or a `no_report` older than a week, and never touches `ok` or
`no_section`; but any half-finished edit left at 03:00 runs.

## P27 · a wrapped row loses its values (L3)

**status** `open` · observed 2026-09-14 measuring L1 · ruled backlog, outside the P22 repair

A label split over two lines with its values on a line of their own:

    細胞治療技術服務
    9,453 0.69 193,040 12.20
    及代檢服務

(3176). The value line carries no label, so the row is dropped silently. 3176 keeps 87.8 and
passes the 50–150 completeness bar: **a plausible total does not prove a complete read**, and the
bar must not be used to cover this gap. 2072's main rows (套管式水下基礎 / 9,533,853 98.65% … /
(Jacket)) are all wrapped, so under L1 it yields no rows — a legal output, not complete evidence.
7853 is the same shape and stays refused. Not in K, not in the expected changes of the second
differential run.

---

## P28 · digits inside an item name end the label

**status** `open` · observed 2026-09-14 measuring L2 · ruled backlog, a lexical boundary, not L2

「腸病毒71型疫苗 524,053 85.47」 (6547): the line label pattern takes no digits, so the line has no
label and the company's main item is dropped; the remaining shares total 14.53 and the section
yields no rows. Not in K, not in the expected changes of the second differential run.

---

## P29 · 部門 naming the row dimension read as a segment column

**status** `open` · observed 2026-09-15 implementing R2-10 · ruled backlog, a P22-2 boundary, no scan

2371 prints 「部門類別 | 部門收入 | 營業比重(%)」, one row per department and a 集團總計 row. Before
R2-10 every 部門收入 cell carried a `$` and the column was not an amount candidate, so the table
read its five shares with no revenue. Once R2-10 reads `$24,342,957`, the column enters the pool
and P22-2's entity words take 部門 as a column-level segment scope: 「segment or region columns
without a total」, scope_unresolved. Here 部門 describes how the rows are cut, not the scope of the
value column. Ruled: 2371 stays scope_unresolved / REVIEW / not written; the fifth differential run
attributes it to R2-10 exposing this boundary, not to an R2-10 regression. No population scan of
部門 headers and no P22-2 change before a separate ruling. Not in K.

---
