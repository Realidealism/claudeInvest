# Reading a mix table: three gates, in order

Design note, 2026-09-12. Written before implementation, from the 22 companies
whose reads changed when `_header_columns` was rewritten — because that rewrite
folded three separate questions into one predicate and got two of them wrong.

The three questions are not variants of each other:

    is this table a revenue mix at all?        → table semantics
    what does each column of it mean?          → header semantics
    where in the data does that column sit?    → data evidence

They run in that order, and **a later gate may never resurrect what an earlier
one rejected.** 2897 王道銀行 is why the order matters: its header is read
correctly, its percentage column is found correctly, its values are valid
percentages — and the table is an income statement. No amount of column-level
correctness makes it a product mix.

---

## Gate 1 — table semantics: is this table eligible?

Runs on the table's **label column**, before any header or column analysis.

**Reject the table** when more than half its data rows' labels fall in one of
these vocabularies. Rejection is of the whole table, never of individual rows:
2897's income statement kept three valid-looking rows after its worst ones were
dropped, and won on those three.

| vocabulary | rejected because | observed |
|---|---|---|
| income statement | 營業收入 / 營業成本 / 營業毛利 / 營業費用 / 營業損益 / 稅前 / 稅後 / 所得稅 / 淨利 / 淨損 | 4726 永昕, 2897 王道銀行 table 0, 3017 奇鋐 p.4 |
| geography | 地區 / 區域別 / 內銷 / 外銷 / 亞洲 / 美洲 / 歐洲 / 中國 / 台灣 / 縣市名 | 6451 訊芯 p.80, 2545 皇翔, 3633 云光 |
| counterparty | 甲乙丙…公司 / A公司 / 被投資公司名稱 / 客戶行業別 | 3709 鑫聯大, 1338 廣華, 3633 云光 |
| change analysis | 增減金額 / 變動比率 / 前後期 | 4726 永昕 cash-flow table |

**Not** a rejection criterion: the values. 2897's 13 / 5 / 5 are real
percentages of a real thing — just not of a product mix.

---

## Gate 2 — header semantics: what does each column mean?

Classify every header cell as **AMOUNT / PCT / CHANGE / OTHER**.

    AMOUNT   金額 · 營業收入 · 淨額 · 營收 · 銷售額
    PCT      比重 · 營收比重 · 百分比 · 占比 · 佔比 · 比例 · % · ％
    CHANGE   變動比率 · 變動 · 增減 · 成長率 · 年增 · 年減 · YoY · 增減百分比

**CHANGE beats PCT on the same cell.** 3017 奇鋐's header reads 「變動比率」,
which contains 比率 and therefore matches PCT today — and its column holds
94.59, a perfectly legal percentage that is a growth rate. The current
vocabulary cannot tell them apart, and the values never will:

> A numeric range can establish that something **may be** a percentage. It can
> never establish that it **is a share of revenue**.

Where the year row spans several columns, the sub-header names them and the
year row names the span (2884 玉山金: `2025` over `金額`/`%`, then `2024`).

---

## Gate 3 — data evidence: where is that column?

The invariant this gate exists to state:

> **Data evidence is veto evidence, not mandatory evidence.**
> Absence of supporting evidence is not evidence against.

Let *k* be the column Gate 2 named as PCT.

| what the data at *k* looks like | action | why |
|---|---|---|
| at least one numeric value, all within 0–100 | **use k** | the header said so and nothing contradicts it |
| values present, any outside 0–100 | **contradiction** → search the year span for a ratio column | the header index points at something that cannot be a share |
| no numeric values at all in any data row | **unaligned, not wrong** → search the span | 2637 慧洋's spacer columns put 金額 at header index 4 and the amounts at data index 3; the header's claim that a ratio column exists still stands |
| the table has no data rows to inspect | **use k** | 1432 大魯閣 — a percentage-only table never contains a value above 100, so the header scan consumed every row and left nothing to check |
| only one data row | **use k** | 3526 凡甲 「連接器 100」, 3588 通嘉 「電源管理IC 100」 — one product is still a mix |

When Gate 2 named **no** PCT column at all, Gate 3 may **supply** one: search
the year span for a column whose values lie in 0–100 and whose sum — with or
without its largest value, to absorb a 合計 row — lands near 100. This is the
only case where data creates a column rather than vetoing one. It is what reads
2884 玉山金, whose 金額/% sub-header row was empty, and 1584 精剛, whose ratio
column carries no % sign.

---

## What the rewrite got wrong, stated as the rule it broke

    header evidence AND data evidence   →  wrong
    header evidence VETOED BY data      →  right

Requiring the data to *confirm* the header meant that a table with too little
data to confirm anything lost its ratio column entirely. 15 tables that had been
read correctly stopped being read: the percentage-only tables (no value ever
exceeds 100, so the header scan swallowed the whole table) and the single-line
tables (one value, and the column test demanded two).

The downstream shape of that failure is worth recording separately, because it
does not look like a column bug from the outside: when the product table stops
yielding rows, `parse_pdf` walks on and the next table wins. 6451 訊芯 looked
like a geography misclassification and was nothing of the kind — its product
table on pages 9 and 61 had been broken by Gate 3, and the geography table on
page 80 was simply the next one standing.

---

## Outcome of the 23, measured through `parse_pdf`

The first version of this table was produced by a script that defined the winner
as **the first table yielding any rows** and then reported that definition's
behaviour as the parser's. It is not the parser's: `parse_pdf` already refuses to
let a table without percentages end the search, so a pct-less table never was a
winner and the "winner-takes-first" problem the first run described did not
exist. Every row below comes from running `parse_pdf` itself under each reader.

| outcome | n | cases |
|---|---|---|
| identical | 12 | 1213, 2913, 4528, 1432, 3526, 3588, 3122, 6451, 4529, 2516, 2851, 6021 |
| strictly better | 7 | 3017 (損益表 94.59 → 3C電子產品 100), 4197 (損益表 → 醫材測試儀 89.02), 6533, 6160, 6661, 6803, 4550 |
| honest downgrade | 1 | 4726 (cash-flow table read as amounts → `no_section`) |
| **regression, closed** | 1 | **2897** — and closed against the sign of a row, see below |
| open, pending ruling | 2 | 1442 (Gate 1 boundary), 2882 (scope, not selection) |

6533 and 6021 were regressions and are repaired by the two mechanisms above;
6533 now reads one line *more* than the line reader it displaced (其他收入, with
no percentage disclosed). 2851's geography table is refused by the completeness
check even with Gate 1 switched off, so the two mechanisms agree about it.

The three regressions as they were found, stated as losses. Two are now closed:

| case | before | after | lost |
|---|---|---|---|
| 2897 王道銀行 | 企金授信 66 · 個金放款 15 · 財管 2 · 股債投資 4 · 其他金融交易 14 | 利息淨收益 47 · 手續費淨收益 13 · … · 淨收益 100 | the business-line table; Gate 3 made the income statement readable and Gate 1 did not hold it (4 of its 9 labels carry 損益, which is not a majority) |
| 6533 晶心科 | 矽智財授權 65 · 權利金 19 · 矽智財維護 16 = 100 | 矽智財授權 65 · 矽智財維護 16 · 其他收入 None = 81 | 權利金收入 19% |
| 6021 美好證 | 客戶夥伴部 55.22 · 投資部 39.85 · 投銀 0.67 · 期貨 1.57 · 其他 2.69 = 100 | 投資部 62.44 · 期貨部 1.02 = 63.46 | three revenue lines |

2897 is not fixed by adding words to Gate 1. Its two tables are the two axes a
bank discloses, and which of them is the industry mix is a taxonomy question, not
a parser one — `db/TAXONOMY_GAPS.md` G20 rules that a bank's axis is its
金融功能／子公司型態 and that income types never rise to an industry axis. The
parser implements that; it does not anticipate it.

**2882 國泰金 is not a selection bug.** An earlier note here claimed `parse_pdf`
let a percentage-less table end the search. It does not: such a table is kept
aside as `amounts_only` and only used if every reader fails. 2882's page 160 is a
valid candidate with valid percentages, and it terminates legitimately. What is
wrong is its **scope** — 人壽保險 51.2% is one subsidiary's mix, not the group's,
and the filing gives no subsidiary's share of the group. That is R020②, a
measurement question, and it belongs in the R020 backlog rather than here.

---

## After the gates: the rows have to be the table's rows

The gates decide whether a table is eligible and what its columns mean. Neither
question is about whether the rows that came out are all of them, and two cases
showed that a table can pass every gate and still be read incompletely.

**A logical row split in two.** pdfplumber returns 6533 晶心科's 權利金收入 as a
label carrying only the prior year's figures, followed by a nameless row carrying
the wanted year's. Each half fails on its own — one has no value in the chosen
columns, the other has no name — so the line vanished and the table summed to 81.
`_join_split_rows` rejoins exactly that shape: a label with no value, immediately
followed by a value with no label. Nothing else is joined.

**The table's own total as the completeness test.** 6533 accepted 81 of a declared
100; 6021 美好證 accepted 63.46 of a declared 100, because extraction lost three
departments' labels *and* their percentages — nothing in the table could recover
them. A read that contradicts the issuer's own 合計 is incomplete, and an
incomplete read must not end the search: refusing 6021's table is what lets
`parse_pdf` reach the line reader, which has all five departments.

    a threshold invented for the purpose      →  arbitrary
    the number the filing itself prints       →  self-evidencing

The tolerance is `max(2.0, 0.5 × rows)`, and it exists because filings round:
2897's business lines are printed as whole numbers and sum to 101 against a
declared 100. A check that cannot absorb that would discard correct reads.

**Why this is not a fourth gate.** It does not ask whether the table is eligible
or what a column means; it asks whether the extraction was lossy. It runs on the
rows, after the gates have finished, and its only power is to withdraw a result —
which returns the page to the readers that come after it. The gate order is
untouched, and no gate heuristic was widened to fix a row-loss bug.

**A symptom it covers that it should not have to.** 6451 訊芯-KY's region table
still passes Gate 1 — only 美洲 and 其他 reach the majority test, because 亞洲's
row carries no value in the wanted year, and one of two is not a majority. It is
now refused for summing to 52.91 against a declared 100. That is the right answer
by the wrong route, and the Gate 1 hole is recorded in
`test_a_region_table_short_of_its_own_total_is_refused`.

---

## A table may not count its own total as one of its parts

2897 王道銀行's income-statement table ends on a row called 淨收益 — 6,690,492,
exactly the eight rows above it added up — and nothing in it says 合計. So the
reader emitted the total as a ninth product at 100%. Four further rows were
dropped by `_is_context_item`, which left the accepted rows at 125: inside the
50..150 window, so nothing caught it. And there was no declared total to check
against, because the declared total was being read as a product.

`_unnamed_total_row` settles it with arithmetic and not one word of vocabulary:

    a row whose amount equals what the other rows add up to is not one of them

Three guards keep a coincidence from qualifying — the row must also claim 95..105
percent, the table must have at least four rows carrying amounts, and **if the
filing names its own total anywhere, nothing is inferred at all**. That last one
is the important one. Without it the parser slides from recognising structure to
inventing it, and 2897's own business-line table — whole numbers summing to 101
against a declared 100 — is exactly the shape that would invite a hunt for a
total among the parts.

**Why this did not become Gate 1 vocabulary.** `db/TAXONOMY_GAPS.md` G20 forbids
rejecting a table for being income types: 玉山金 discloses no business line at
all, so 利息收入 / 手續費收入 / 其他淨收益 is the only composition its filing
offers, and a rule aimed at 王道 would take 玉山 with it. What is wrong at 2897
is arithmetic, not subject matter. Gate 1 was not touched.

### Measured through `parse_pdf`, 90 cached filings

    attempted 90, recorded 90 · unchanged 87 · changed 3 · measurement errors 0

    2897 王道銀行  the income-statement table → 企金授信 66 · 個金放款 15 ·
                  其他金融交易 14 · 股債投資 4 · 財富管理 2, identical to the
                  answer already in the database
    2881 富邦金    富邦證券's 經紀及財管 75 → 富邦人壽's 壽險保費收入 72
    2883 凱基金    nothing → 凱基人壽's policy mix

No non-financial filing changed. 2881 and 2883 are not right-or-wrong questions:
both arms are one subsidiary's mix rather than the group's, which is R020② and
stays in that backlog.

**What it did not cover, and now does.** The detector is arithmetic, so it can
only work if it can read every amount in the table. `_to_num` returns `None` for
`(1,485,732)` and `△x`, and a row it could not read simply never entered the sum:
rewriting one ordinary amount in the real `2897_income_types` fixture into
parenthesised form took `_unnamed_total_row` from `10` to `None`, and the 淨收益
total came back as a product with the rows summing to 125. The original
regression, unchanged, triggered by the ordinary way banks print a loss.

Closed on 2026-09-13. `_accounting_num` reads the negatives the filing prints,
locally to this arithmetic — the shared `_to_num` stays strict, because widening
*it* was measured across 90 filings and made two companies worse. And an amount
that still cannot be read now abandons the inference rather than being skipped,
because a row silently missing from the sum lets another row match it by
accident.

The second half is what took three attempts, and the failures are worth keeping
because each looked reasonable:

    bail on any unparseable amount   →  2897's header row says 2025年度; detector gone
    also skip row 0                  →  its header is two rows deep; row 1 says 金額
    also skip cells matching a year  →  ROC years are 3 digits, so 100 is both a
                                        year pattern and an ordinary amount

What holds is narrower than any of them: skip the header row, treat a cell with
no digit at all as claiming no amount, and abandon the inference only when a cell
that *does* carry digits cannot be read. A row's identity is not its sign, and a
label is not an unreadable number.

## Two defects this round recorded rather than fixed

**Gate 1 does not consume the label column.** `_label_col` decides where a row's
name is — 2069 運錩's supplier table holds serial numbers in column 0 and names in
column 1 — and `_rows_from_table` uses it. `_table_axis` does not: it takes each
row's first non-empty cell, so its body is 其他 and 進貨淨額 and the four
A公司/B公司 rows are never counted. Ten cached filings carry a supplier or customer
concentration table of this shape, and all ten became readable this round, because
until now their own 進貨淨額 100% row pushed the sum to 200 and the 50..150 window
refused them by accident. Accident is not a gate.

No reader outcome changes, and that was measured rather than argued: the change
was implemented, run against the same 90 cached filings at reader level, and
reverted — `attempted 90, recorded 90 · unchanged 90`. Not one filing's mix moved,
because `parse_pdf` reaches none of those pages today. So this
is a latent defect, not a regression of this release — and the fix is not to bolt
`_label_col` onto Gate 1, which only swaps one positional heuristic for another.

**Corrected 2026-09-14 — it is not latent.** The "reaches none of those pages" claim was measured on 90 cached filings, and the full 2302-company run reached one the sample did not contain. 3633 云光's page 47 is `主要供應商資料`: column 0 numbers the rows 1–4, column 1 names the suppliers, and the share column is `占全年度進貨淨額比率(%)` — purchases, not revenue. It is 2069's shape exactly, and the reader returns it as the company's mix (`環鴻(昆山) 12.23 / 日亞 8.05 / 州巧(廈門) 5.86 / 其他 73.86`). It surfaced in reconciliation as a classification impact, because the stored rows it would replace carry the Core weight. A sample proves a defect present; it does not prove one absent.
Gate 1 should consume a column contract — label, amount, pct, change — decided
once by the structure parser ahead of it. Pinned by
`test_defect_gate_1_does_not_consume_the_label_column`.

**`_to_num` cannot read accounting negatives, and the obvious fix is worse.**
`(4%)` and `△4%` both return `None`; only `-4%` parses. 2881 富邦金's bank table
loses its 其他 (3,956) / (4%) row and accepts 104 against a declared 100. Reading
`(x)` as `-x` was measured over the same 90 filings and **must not be adopted**:

    attempted 90, recorded 90 · reader unchanged 88 · changed 2, both worse

    2897  the business-line table loses its percentage column entirely
    4726  a cash-flow statement becomes readable as amounts

A negative share is legitimate in these tables — 權益法轉投資 loss lines carry one
— and once parsed it fails the 0..100 test that qualifies a percentage column, so
the column is disqualified and the whole table degrades to amounts. Any fix has to
act where the completeness arithmetic is done, not in the shared number reader.
The table-level count for the same run was 74 of 90 changed, which is why table
counts are not decision evidence: 72 of those tables are never reached.

---

## Reader correctness is not write authority

> **Producer ownership is write authority, not candidate classification.** That a
> reader can read a company does not mean the reader owns the right to overwrite
> that company's existing evidence.

The distinction matters most where it is least visible. In a candidate snapshot,
ownership looks like a label on a row — a stratum, something to group by. It is
not: it is the question of who is permitted to write. The two cases below differ
in every respect except that one, and that one is what decides both.

Ruled 2026-09-12, after the full re-run's snapshot produced two results that were
both *right* and both ineligible to be written.

A candidate snapshot describes what would happen if this reader were applied. It
does not thereby acquire the authority to overwrite an existing adjudication, or
to touch rows another producer owns. Those are separate grants, and the reader
has neither by default.

    reads the filing correctly        →  a claim about the document
    may overwrite what is stored      →  a claim about who decided, and who owns

Two cases this round, which look unrelated and are the same thing:

**2884 玉山金 — an existing adjudication.** Migration 110 retracted its four
income-statement captions deliberately. The reader now reads them through the
table path with real percentages, so the evidence has genuinely changed. That
earns 2884 a reconciliation; it does not earn the snapshot permission to move the
status back to `ok` on its own.

**1103 and 1201 — another producer's rows.** Their rows were written by the
narrative scrapers. `parse_pdf` cannot produce that method at all, so whether the
reader is silent or speaks, it is acting on rows it never owned. `save()`'s
retract predicate cannot see this, because it matches on `source_file` shape
rather than on provenance — `db/PARSER_BACKLOG.md` P11.

Both gates are enforced in the release predicate rather than argued: producer
ownership, and adjudication conflict derived from the stored before-state rather
than from a hard-coded list of known cases. A hard-coded 2884 would have closed
the case it already knew about and missed its siblings.

---

## What the full re-run's denominator rests on

The 2302-company re-read is the evidence the database write is judged on, so two
defects in the *measurement* — both caught by a twelve-company smoke test before
the run started, neither by reading the code — belong to its provenance.

**The before-state and the after-state were not comparable.** Stored rows come
back from JSON as lists ordered by `item`; the reader returns tuples in reading
order. `[('a', 1)] == [['a', 1]]` is False either way, so 1213 大飲 — identical
content, different order and type — was reported `changed`. Left in, almost every
company would have been reported changed and the classification behind the
release decision would have been worthless. The verdict now normalises type and
order before comparing.

**Two producers shared one population.** 1303 南亞's 21 rows carry method
`narrative_text` and were written by `annual_report_narrative_text.py`;
`parse_pdf` cannot return that method at all. Re-reading them through `parse_pdf`
and scoring the empty result as `lost` compares against the wrong producer, and
would have manufactured 141 losses that never happened. Those companies stay in
the population — production `save()` still acts on them — but they are recorded
as their own stratum and kept out of the reader's score.

Both are one failure with two faces, and it is the failure Rubric 8 exists for: a
denominator that looks closed while counting the wrong things. A count that
reconciles is not the same as a count of the right population.

---

## Why the first round of tests did not catch any of this

`WANGDAO_PL` in the first version of `tests/test_table_gates.py` was **typed by
hand from a reading of the page**, not extracted from the PDF. The real table has
nine data rows; the fixture had three, and the three happened to be the ones that
carry 損益. So the fixture tripped Gate 1, the test asserted that it did, and the
suite went green while the actual filing did the opposite.

    a fixture that was transcribed rather than extracted
    tests the transcription, not the filing

Every fixture in that file must come from `page.extract_tables()` on a cached
filing. A green suite over invented input is worth less than no suite at all,
because it converts an open question into a settled one.

---

## Where each clause is enforced

Every rule above is a test in `tests/test_table_gates.py`, so the spec cannot
drift from the reader without the suite going red. The four load-bearing ones:

| invariant | test |
|---|---|
| the gates are ordered — Gate 1 is not outvoted by a clean Gate 2 and 3 | `test_gate_1_cannot_be_overridden_by_a_clean_gate_2_and_3` |
| rejection is of the table, never of rows | `test_rejection_is_of_the_table_not_of_rows` |
| Gate 3 may veto or relocate, never require | `test_a_table_of_percentages_only_is_read`, `test_one_product_is_still_a_mix`, `test_an_empty_named_column_is_unaligned_not_wrong` |
| CHANGE beats PCT on the same cell | `test_change_beats_pct_on_the_same_cell`, `test_a_growth_rate_is_never_a_revenue_share` |

**The financial-issuer boundary is not settled and no test asserts it.** The
earlier claim here — that 2884 玉山金's 利息收入/手續費收入 table is a mix while
2897 王道銀行's 利息淨收益/手續費淨收益 table is an income statement — put the
same kind of table on both sides of the line. Both issuers disclose two axes, and
which one is the industry mix is decided in `db/TAXONOMY_GAPS.md` G20, not here.
Until that is closed, Gate 1 gains no financial vocabulary.
