# P22 repair — line-reader contracts R1-3′ / L1 / L2

**Status:** **ruled 2026-09-14** — R1-3′, L1 and L2 adopted; 6910 refuses (no joining rule);
2072's no rows accepted as a legal output, not as complete evidence; L3 wrapped rows and digits in
a label are backlog (P27, P28), outside this revision, outside K and outside its expected changes.
Implemented after the ruling (section 6). Before it, L1 and L2 existed only as a measurement
prototype (`tmp/rejudge/p22r2/proto_lines.py`), compiled in a separate process. DB 0 writes, scheduler `InvestAnnualReportSweep` Disabled, parser uncommitted,
second full differential run not started (ruled 2026-09-14: not before R1-1, R1-2, R1-3, R1-5,
L1 and L2 are all explicit contracts).

Revision 1's R1-4 is withdrawn as a name: the reading of 1325, 1522 and 6547 showed no
single-period header problem. It showed two shapes, L1 and L2, and the R1-3 scan showed that
R1-3 as ruled is itself an instance of L2.

---

## 1 · R1-3: what the real scan found

**Scan.** Every cached filing (2302, closed, 2 read errors recorded): lines of the 營業比重 window
carrying a standalone 「-」/「－」/「—」. 691 lines in 564 companies
(`tmp/rejudge/p22r/placeholder_hits.txt`, classified by `tmp/rejudge/p22r2/dash_kinds.py`):

| where the dash sits | lines | companies |
|---|---|---|
| page number 「- 87 -」 | 482 | 460 |
| among the values | 150 | 106 |
| bullet before a label | 31 | 19 |
| prose, ranges 「250W - 1600W」 (a few value lines misclassed, e.g. 2101 營建(註1)) | 28 | 15 |

**Is the dash an empty value?** Yes, where it sits among values. Two meanings, both "no number":
the period has no value (1806 「其他 2,489 0.08 - - - -」, 5301 「食材批發 - - 110,204 30.01」,
6467 「勞務收入 — — 8,328 99.02」), or the share rounds to nothing beside a small amount
(2013 「其他營業收入 18,689 -」, 2641 「客戶合約收入 31,604 1.91 30 -」). It is never a number.

**R1-3 as ruled is defective.** Its pairs decide each value's measure by form (≤100 unmarked is a
share). On the same page text, placeholders on against off:

| company | line | R1-3 as ruled | correct |
|---|---|---|---|
| 2641 | 客戶合約收入 31,604 1.91 **30 -** | row 客戶合約收入 **30%** | 114 amount 30, share none → no row |
| 6648 | 其他收入 - - **2 -** | row 其他收入 **2%** | 114 amount 2, share none → no row |
| 6856 | 其 他 **10 0.00%** - 0.00% | whole section **refused** (two shares in a pair) | read (was read before R1-3) |
| 7834 | 其他營業收入 **59 0.24** - - | whole section **refused** | read (was read before R1-3) |

Every one of these headers names the measures in order (「營業額 營業比重(%) 營業額 營業比重(%)」,
「營收淨額 營業比重 營收淨額 營業比重」). This is exactly L2: form is not measurement identity.

**Company-level impact of R1-3 as ruled: 0 of 979.** The alignment ran on 71 pages and changed
page-level output on 55, but in every one of those companies a table owns the section, so the line
reader's output never reaches `parse_pdf`'s result (`proto_run`: `base` vs `now` identical for all
979 companies that can reach the line reader).

**Proposal R1-3′.** A standalone dash among values is a **value position carrying NULL**. Its
measure — like every value's — comes from the header's measure identity (L2), never from form. A
NULL target share skips the row (no borrowing, as ruled). Where the header names no measures, a
line with a placeholder is read as before R1-3 (the form-decided pairs are removed).

## 2 · L1: where the line evidence block ends

**Mechanism.** The line reader reads a 2500-character window from 營業比重. When the table ends,
it keeps reading: prose (1325 「98 年 5 月」), the next section's product table (3042 「3.2 x 2.5mm」),
an industry statistics table (1522). A line there can refuse the section, or become a row
(8210 「◆ 搭配 21“OCP 機櫃」 21%; 6863 「資料來源:IDTechEx (2025年7月)」 7%).

**Measured on the 139 companies that use the line reader** (`line_block_probe.py`, per-line
records; `line_block_analyse.py` compares boundary rules on the same records):

| rule | refusals cleared | pages losing rows | wrong losses seen |
|---|---|---|---|
| T first 合計/總計 line | 9 | 5 | — |
| H first section heading | 14 | 6 | 1316 (items print 「C.」), 1905 |
| min(T, H) | 15 | 8 | 1905: a table-of-contents sidebar 「三、…從業員工資料」 interleaves the rows, cuts 其他 |
| T, else H | 14 | 7 | 1522 kept refused: the next 合計 belongs to the industry table |
| **own total (T100), else H** | **15** | **5** | **none** |

**Proposed contract L1.** The line evidence block starts at the first value line and ends at:
1. the table's **own total line** — its label is 合計 or 總計 and it prints a 100 share
   (100 / 100.0 / 100.00 / 100%); or, without one,
2. the line before the first **section heading that carries no values** (「3.」「(3)」「3、」「A.」,
   not 「3.2 x」, not an item such as 「C.保險經紀佣金收入 11.04%」).

Lines past the boundary are not evidence: they neither become rows nor refuse. A total that is not
the table's own (1522's industry 合計 printing growth rates) does not close the block.

**Company-level impact (979 companies, `parse_pdf` with the prototype): 22.**
- refused → read, 15: 1314 1325 1460 1522 2349 2910 3051 3176 5907 6555 6582 6735 6921 6923 7912
- refused → no rows, 2: 2072, 2527
- rows changed, 5: 1316 (junk rows 樹脂 1% ×n dropped), 4108, 6184 (「Android TV 14」), 6863
  (「資料來源」), 8210 (「◆ 搭配 21」)

Read against the source: 6923 takes the 114 group of three; 6735 reads as printed (the filing's
own 114 shares total 94.0). **Released is not accepted**: all go to the differential run's
attribution and source review.

**Residual, not addressed by L1.** A window with no table before its prose (3042, 3051, 6709): no
value line, so no boundary; a header read as data (3665 「比重 比重 5」, 1460 「1 1 4年」).

## 3 · L2: measure identity from the header

**Mechanism.** The line reader decides a value's measure by form: a % mark or an unmarked value up
to 100 is a share, a larger one an amount; 0 and 0.00 count as no value
(`(_to_num(...) or 999) <= 100`). Small amounts become shares (6547 「其他 64 0.01」, 6856, 7834),
zeros vanish (3689 「0 0」, 6787 「303 0.00」), and a measure-first layout cannot be read at all
(6144, 9950: 「營業收入淨額 百分比」 over 「113 114 113 114」).

**Proposed contract L2.** When a header line above the first value line names **both an amount and
a share**, each value position takes its measure from that header:
- measure words: amount 金額 · 營業額 · 營業收入(淨額) · 營收淨額 · 銷售金額 · 銷貨淨額 …; share 比重 ·
  百分比 · 比例 · 比率 · % (a % after a share word belongs to it); change 成長率 · 增(減) · 變動;
  「佔/占 … 比率/比重/百分比」 is **one** share (6184 「占本年累計營業收入百分比」, 6918);
- positions combine with the header's periods: a repeated unit (金額 % 金額 %), a measure block over
  a period row naming every position (6144), or one unit per period; placeholders are positions;
- the identity **exists only if it accounts for at least one value line** of the block; a header
  that fits no line is not an identity, and the lines are read as before (1582's header wraps over
  four lines, 4139's is garbled, 1303 and 8411 carry quantity columns);
- the target share comes from the one (fiscal year, share) position; revenue from the one (fiscal
  year, amount) position, NULL if that position is a placeholder or the amount carries a scale unit
  such as 億 (3388 「73.64億」);
- a value contradicting its position refuses the line: a % under an amount, an unmarked value over
  100 under a share; a line whose positions do not fit refuses.

**Company-level impact on top of L1: 8.**
- refused → read, 2: 3689 (「電源及LED照明 0 0 143,229 2.31」), 6021 (four periods, 「投資銀行部 149 -」)
- rows changed, 1: 1325 (「材料加工」 revenue NULL → 65)
- refused → no rows, 2: 6547 (see below), 6787 (the filing's 114 share column totals 2.98)
- refusal reason changed, 2: 6194 (header 「114年度營業收入 113年度營業比重」 still refused), 7853
- **read → refused, 1: 6910** — 「授權收入 73 ,092 19%」: extraction split the number, the line fits
  no position, and the section refuses. Before, the reader took 19% with no revenue.

Page level, where a table owns the section and the result does not reach `parse_pdf`: 2641 and
6648 lose their false rows; 6856, 7834, 5301, 6467, 9950, 6144, 1806, 1905 read.

## 4 · Shapes found on the way, not proposed

- **L3 wrapped rows**: a label split over lines with its values on a line of their own
  (3176 「細胞治療技術服務 / 9,453 0.69 193,040 12.20 / 及代檢服務」, 2072, 7853). The row is dropped
  silently; 3176's remaining shares total 87.8 and still pass the 50–150 completeness bar. 2072
  goes from refused to no rows under L1 because its main rows are all wrapped.
- **Digits inside a label**: 6547's 「腸病毒71型疫苗 524,053 85.47」 matches no label, so the main
  item is dropped; the remaining shares total 14.53 and the section yields no rows.

## 5 · Totals: the parser today against R1-3′ + L1 + L2

29 of 979 companies change (`tmp/rejudge/p22r2/proto_analysis2.txt`):

| change | n | companies |
|---|---|---|
| refused → read | 17 | 1314 1325 1460 1522 2349 2910 3051 3176 3689 5907 6021 6555 6582 6735 6921 6923 7912 |
| refused → no rows | 4 | 2072 2527 6547 6787 |
| rows changed | 5 | 1316 4108 6184 6863 8210 |
| refusal reason changed | 2 | 6194 7853 |
| read → refused | 1 | 6910 |

Fixtures captured from the cached filings, for the tests written after the ruling
(`tests/fixtures/mix_lines.json`, keys `lc_*`, 14 pages): 1806, 5301, 6467 (placeholders),
2641, 6648 (the counterexamples to form-decided pairs), 6856, 7834, 3689 (small amounts, zeros),
6144, 9950 (measure-first headers), 1522, 1905, 1316 (block boundary), 6194 (still refused).
The three typed-string R1-3 tests in `tests/test_scope_revision_1.py` test the form-decided pairs
and are replaced after the ruling.

## Decisions to rule

| id | question | recommended |
|---|---|---|
| R1-3′ | a placeholder is a NULL value position whose measure comes from the header identity; form-decided pairs removed | yes |
| L1 | block ends at the table's own total (a 100 share), else before the first value-free section heading; lines past it neither read nor refuse | yes |
| L2 | header measure identity decides measure where it fits a block line; otherwise lines read as before | yes |
| 6910 | a number split by extraction (「73 ,092」): refuse as a misfit, or join 「\d+ ,\d{3}」 before reading | refuse now (no joining rule without its own scan) |
| 2072 | refusal becoming no rows because the remaining rows are wrapped (L3) | accept as no rows; L3 goes to the backlog |
| L3, label digits | backlog, not in this revision | yes |
| rerun | second full differential run with R1-1, R1-2, R1-3′, R1-5, L1, L2 once implemented with these fixtures | after implementation and a green suite |

---

## 6 · Ruling and implementation (2026-09-14)

**Ruled.** R1-3′ ✅ · L1 ✅ · L2 ✅ · 6910 refuses, no joining heuristic ✅ · 2072 no rows accepted
and listed for repair ✅ · L3 wrapped rows ⏸ backlog · digits in a label ⏸ backlog · second full
differential run once the contracts are in and the suite is green, against frozen v2, all hard
gates kept (closure 2302 / one outcome each, identities, determinism 65, K 20 directions, refusal
reasons 100% attributed, refusals outside R > 20 per component → STOP, source REVIEW > 300 →
STOP, a K direction wrong → STOP). L3 is not in K and not in the expected changes.

**2072 is not repaired.** No rows there means the reader did not join broken text into an item;
the evidence was still not read in full, because its main rows are wrapped (P27). A total inside
50–150 does not prove a mix complete: 3176 drops a 12.20% row and keeps 87.8.

**Implementation** (`tmp/rejudge/p22r2/_patch_line_contracts.py`, parser backup
`tmp/rejudge/annual_report_revenue.py.bak-lc`): `_placeholder_groups` removed; `_line_block_end`
(L1), `_line_measure_header` / `_identify_positions` / `_read_by_identity` (L2) added; the line
reader reads `raw[1:end + 1]`, skips the header's own lines, and decides an identified line by
position before any form-based branch. A port of the prototype, so `parse_pdf` over the 979
companies is compared record for record with `proto_run2`'s L1L2 variant.

**Tests.** `tests/test_line_contracts.py`, 20 tests over 19 real pages (`mix_lines.json`, `lc_*`);
against the pre-contract parser 12 fail (the defects) and 8 pass (the controls that must not
move: 1806, 5301, 1905, 1582, 6184, 3388, 6194, provenance). The three typed-string R1-3 tests
are removed from `tests/test_scope_revision_1.py`.

**R1-5 trace.** `tmp/rejudge/p22r2/run.py` records, per page the line reader reads, whether the
outcome differs with L1 and L2 switched off (`line_contracts`); `merge.py` counts it as the
mechanism acting and maps L2's refusal reasons to `L2 measure identity`.
