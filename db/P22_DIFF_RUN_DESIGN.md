# P22 repair — whole-population differential run (design for review)

**Status:** ruled 2026-09-14 (thresholds 300 / 20 / 65 accepted as hard safety stops, not targets). Running.

**What this gate decides.** Whether the repaired reader (P22-1, P22-2, P22-4, P22-3; 832 tests)
introduced a change nobody predicted, and whether its candidate evidence is fit to enter the
next reconciliation. It is not P22's original closure, which is done. It is not write
authority either: a passing run leads to reconciliation → ownership → a new write set → M5 →
M6 → M7, and none of those is decided here.

---

## 1. Inputs, frozen

| input | role | guard |
|---|---|---|
| `tmp/rejudge/full_candidate_v2.jsonl` | **baseline**: the pre-repair reader on the same cached filings (2302 records) | sha256 recorded at run start, and compared again at merge time; never regenerated |
| `tmp/rejudge/pdfcache/` | the same filings v2 read | no network; a missing file is `measurement_error`, never a reader outcome |
| `scrapers/annual_report_revenue.py` | the repaired reader | sha256 stamped into every record; the merge refuses shards whose hashes differ |
| `tw.product_revenue_status` (source_file not null, FY2025) | population | must equal the v2 population, 2302 |
| P22 scan artifacts (§3) | the review set | read-only |

The candidate pipeline is v2's (`read_one` in `tmp/rejudge/full_candidate.py`: `parse_pdf` → `_dedupe` →
`parse_conflict` on conflicting duplicates), with one addition. `ScopeUnresolved` is caught
**before** the generic exception handler and recorded as `scope_unresolved`. Otherwise it would
be mislabelled `parse_failed`. The reader itself is not modified for the run. Mechanism traces
come from wrapping functions inside the run process, as the P22 scans did.

## 2. Diff identity — what counts as a change

Each company gets one **primary class**, from the first test that fires in this order, and every
**flag** that applies. The classes are mutually exclusive, so the counts sum to 2302.

| order | primary class | test (v2 → repaired) |
|---|---|---|
| 1 | `MEASUREMENT` | either side is `measurement_error` |
| 2 | `STATUS` | `cand_status` differs; the transition is recorded, e.g. `ok→scope_unresolved`, `no_section→ok`, `ok→no_section` |
| 3 | `ITEM_SET` | same status; the set of items differs; items added and removed are both listed |
| 4 | `PCT` | same items; a share differs |
| 5 | `REVENUE` | same items and shares; a revenue differs |
| 6 | `METHOD` | same rows; only the method differs (`lines` → `table`, for example) |
| 7 | `NONE` | identical, after normalisation: item text through `_clean`, numbers rounded to 4 places, row order ignored |

**Flags**, several per company: `pct_value→value`, `pct_value→NULL`, `pct_NULL→value`,
`rev_value→value`, `rev_value→NULL`, `rev_NULL→value`, `items_added`, `items_removed`,
`method_changed`, `became_refusal`, `refusal_cleared`.

Status and rows are compared separately, so a company reaching `scope_unresolved` from `ok` is
never counted as "rows removed", and a revenue NULLed by D4 is never hidden inside a share change.

**Mechanism trace**, recorded for every company from the repaired run:

- table path: every `_period_selection` outcome (`target` / `unlabelled` / `no_target`); whether a
  period was established from a non-Arabic label (Chinese numeral, 度, 民國, bare AD year);
  whether `_entity_select` ran and what it chose; whether `locate` hit a tie.
- line path: groups declared and target index (by period or by entity); whether a line did not
  align; whether a revenue was left NULL because amount and group counts differ.
- section: number of candidate tables and refusals; `_own_section`'s pool, evidence groups and
  outcome; whether the owner is **not** the first candidate in page order (P22-3 acted).
- the refusal reason and header text, where there is one.

A company whose trace shows none of these is **contract-silent**: the repair had no occasion to
act on it.

## 3. Expected sets — built by set operations, not by listing names at run time

Two sets, deliberately kept apart.

### K — known to change (a registry of prior rulings, with the direction of each change)

K is **not** assembled at diff time. It is read from rulings already recorded before the repair,
materialised once into `tmp/rejudge/m5/p22_known.json`. Each entry holds the company, the shape,
**the expected direction**, and where the ruling is recorded:

| source (already recorded) | companies | expected direction |
|---|---|---|
| `reconcile.py` `SOURCE_REJECTED`, entries tagged P22 / SCOPE_MISMATCH | 2404, 8472, 9930, 2739, 5310, 2736, 3046, 6741, 2013 | the ruled column/table, or refusal |
| `PARSER_BACKLOG.md` P22 confirmed-shapes table | 2727, 7742, 2332, 1110, 8996, 2492, 4722, 2101, 4131, 7907 | as ruled there |
| the repair's positive fixtures (`p22_*` in the three fixture files) | as tested | exactly the tested value or refusal |

6592 is a confirmed mismatch, but its header is corrupted and the ruling names no direction the
repair must produce. It belongs to R, not K. An entry enters K only with a direction.

Each entry's direction is one of `refuse`, `share→column`, `share+revenue→column`,
`revenue→group`, `table→consolidated`. The merge checks each K member **mechanically** against
its direction: `K_confirmed` (changed as ruled), `K_wrong_direction` (changed otherwise), and
`K_not_applied` (unchanged). The last two are failures of the repair, not items to review.

### R — theoretically affected (review set, never an expected-change set)

Computed from the scan artifacts, using exact files:

```
R_A = { id : P22-A record has any flag }                    # p22_scan.jsonl ∪ p22_shard_{0..7}.jsonl
R_B = { id : P22-B outcome ∈ {ambiguous, source_not_located}
             or verdict.chosen_in_candidates is False }     # p22b_shard_{0..7}.jsonl
R_C = { id : P22-C check.amount_findings non-empty
             or "positional period" ∈ check.risk }          # p22c_shard_{0..7}.jsonl
R_D = { id : whole-filing check outcome = alternate_found } # p22d_shard_{0..7}.jsonl
R   = R_A ∪ R_B ∪ R_C ∪ R_D
E   = { id : repaired trace is not contract-silent }        # from this run
```

Membership in R or E explains **why** a change could happen. It does not accept the change.

## 4. Regression rule

For every company with primary class ≠ `NONE`:

| condition | verdict | action |
|---|---|---|
| id ∈ K, direction matches | `K_confirmed` | none |
| id ∈ K, direction differs, or unchanged | `K_wrong_direction` / `K_not_applied` | **repair failure**; stop |
| id ∉ K, id ∈ E | `REVIEW` | read at the source with `source_view.py` |
| id ∉ K, id ∉ E | `REGRESSION` | **presumed regression**: a change with no mechanism behind it |
| id ∉ K ∪ R ∪ E | `REGRESSION` (unexplained) | highest severity |

**A change outside K is never accepted on plausibility.** A `REVIEW` item becomes accepted only
by a source reading that confirms the new value is the fiscal year's consolidated evidence and
the old one was not. Readings are recorded per company with page and reason.

**Reading budget.** Every `REVIEW` and `REGRESSION` company is read. If that count exceeds 300,
the run stops and reports the counts by class, flag and mechanism **before** any reading, so the
scale is ruled rather than sampled silently.

## 5. Refusal accounting

Every `scope_unresolved` maps to a component through its reason text. The reason strings are the
parser's own, so the mapping is exact; an unmapped reason counts as a closure failure.

| reason (prefix) | component |
|---|---|
| `no target period; the header names …` | P22-1 period |
| `N shares on a line and no period labelled` | P22-1 line period |
| `N columns under the target period and none identified` | P22-1 tie / several target columns |
| `… groups named` (shares do not align) | P22-1/P22-4 line alignment |
| `… under the target period` (entity refusal inside a period) | P22-2 |
| `N consolidated or total columns`, `segment or region columns without a total`, `N share columns and no period or entity …` | P22-2 |
| `target consolidated scope unavailable` | D2 — column-level (P22-2) or table-level (P22-3), told apart by whether the refusal came from `_own_section` |
| `N tables carry the target scope as different evidence`, `N unlabelled tables carry different evidence` | P22-3 |

Reported for each component: the count; the prior status (`ok` / `no_section` / other); membership
in K, in R∖K, and outside R; membership in the current write set (411) and in the
exposure-cited companies; and every known refusal case (7907, 4116, 3046, 2739) found where
expected.

**Too-strict guard.** The run is flagged `CONTRACT_TOO_STRICT`, and reading does not start, if
either: refusals outside R exceed refusals inside R; or any single component's refusals outside
R exceed 20.

## 6. Closure invariants (all asserted by the merge)

1. 8/8 shards closed; each shard recorded exactly its assigned ids.
2. 2302 records = 2302 unique = the population; one record per company.
3. `cand_status` ∈ {`ok`, `no_section`, `scope_unresolved`, `parse_conflict`, `parse_failed`,
   `measurement_error`}.
4. **Identities are kept distinct** (ruled 2026-09-14), and the report prints each one:
   - *baseline input identity*: sha256 of `full_candidate_v2.jsonl` and of `before_full.json`,
     taken at run start and again at merge. They must be equal, because the run may not
     rewrite what it is compared against.
   - *before candidate identity*: per company, a hash of the normalised v2 record (status,
     method, rows).
   - *after candidate identity*: per company, a hash of the normalised repaired record
     (status, method, rows, refusal reason, mechanism trace).
   - *code identity*: sha256 of the parser and of the three fixture files, stamped into
     every record. Every shard must carry the same values.
5. **Determinism:** the first 65 ids (a separate run) are re-read after the shards and must
   match their sharded records on the whole after-candidate identity: status, method, items,
   shares, revenues, refusal reason and header, and mechanism trace. Agreement on
   success/failure alone does not count.
6. The primary classes sum to 2302. The K verdicts sum to |K|. Refusals by component sum to all
   `scope_unresolved`, with zero unmapped.
7. `measurement_error` is reported apart and never counted as a reader outcome.

## 7. Output

- `tmp/rejudge/p22r/candidate_shard_{0..7}.jsonl`: v2's record schema, plus `trace`,
  `parser_sha256` and `refusal_reason` / `refusal_header`.
- `tmp/rejudge/p22r/diff_report.txt`: the invariants; class × flag counts; the status transition
  matrix; the K verification table; `REVIEW` and `REGRESSION` lists by mechanism; refusal
  accounting; write-set and exposure intersections.
- `tmp/rejudge/p22r/read_list.txt`: the companies to read, with the page and the mechanism that
  acted.

Estimated runtime: 2302 × 2–10 s over 8 shards, about 30–45 minutes.

## 8. After the run — not decided here

`DIFF PASS` means: no repair failure in K, no unexplained regression, every `REVIEW` item read and
accepted or rejected with a reason, and the too-strict guard not tripped. Only then is the
repaired candidate taken into reconciliation v3 against the frozen `before_full.json`: ownership,
the new write set, then M5 → M6 → M7. The scheduler stays disabled and the parser uncommitted
until those are ruled.
