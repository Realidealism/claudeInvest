# Reconciliation: turning a candidate read into a permitted write

The parser produces candidate evidence. This document is the decision model that
stands between that candidate and the database. It exists because two things that
look the same are not:

    the reader read this company correctly      a claim about a document
    the reader may change what is stored        a claim about who decided

> **Reader can propose evidence; only ownership permits mutation; only
> adjudication permits classification change.**

Apply does not judge. It executes a reconciliation plan in which every company has
exactly one action. The model lives in `tmp/rejudge/reconcile.py` while it is being
settled. Every rule below was ruled explicitly (2026-09-13/14) or extends a rule
`save()` already enforced.

---

## Where this sits

    M1  parser release                    done
    M2  reconciliation model              done
    M3  ownership O1–O5                   done
    M4  evidence authority                done
    M5  classification proposals          done (2026-09-14); round 1 ruled and APPLIED 2026-09-15 (below)
    --  P22-A                                        mechanical PASS / semantic FAIL
    --  P22-B                                        PASS / 4 confirmed WRITE violations, removed
    --  P22-C                                        mechanical PASS / 4 confirmed WRITE violations, removed
    --  high-risk whole-filing closure               done: 120/120, 0 new mis-selections, 0 hold
    --  P22                                          CLOSED (2026-09-14)
    --  write set                                    421 → 415 → 411, post-P22 candidate
    --  parser                                       repaired (P22-1..4, R1, R2-1..10); fifth 2302 diff PASS,
                                                     REGRESSION 0; committed c0ea32dd
    --  reconciliation v3 (2026-09-15)               PASS on the repaired candidate (V3-1..V3-6 ruled);
                                                     write candidate 570 → M5/O5 final pass → 566
                                                     (4 to M5: 3229 3555 7750 6937; 6229 to M5 from v3)
    --  final-write-set conditions (all 0, each read at the source):
          A confirmed scope mismatch ∩ WRITE · B confirmed scope ambiguity ∩ WRITE ·
          C amount/pct scope mismatch ∩ WRITE · source_not_located ∩ WRITE ·
          measurement_error ∩ WRITE · plus no new semantic scope failure in the high-risk subset
    --  a diagnostic scan never changes a reconciliation action: observation → affected set
        → read at the source → decision → reconcile.py
    M5  write-set finalisation            done in v3 (566)
    M6  write authority manifest          built 2026-09-15, 566 rows, consumed by M7:
                                          tmp/rejudge/p22r5/m6/ (manifest.jsonl sha 5dbd7918…,
                                          audit_sidecar.json, SHA256SUMS; read-back 10/10 PASS)
    M7  transactional apply               APPLIED 2026-09-15, transaction 801955: 566/566 preflight,
                                          deleted 1938 / inserted 1920 / status 566, verified 566/566
                                          before and after commit; parser commit c0ea32dd
                                          (blob afc4071a…, executed bytes ab79af66…);
                                          audit tmp/rejudge/p22r5/m7/
        (reclassification of the ~500 classified companies follows M7)
    M5  round 1 (2026-09-15)              11 queued: 9 APPLIED in transaction 801960 (evidence 9/9,
                                          exposure corrected 14 + added 1, verified before and after
                                          commit), then db.fill_weight (15/15 weights as planned);
                                          db/industry_data.py committed 79dfcb95. 6229 is the one
                                          structural change (consolidated table; A4-12 Major added).
                                          6582 and 8027 HOLD for taxonomy rulings. check_industry
                                          coverage 161 -> 162 is 4754's corrected FY2025 coverage
                                          (70.9% -> 62.3%): an expected work-list finding, not a
                                          regression; no rollback. Audit tmp/rejudge/p22r5/m5/apply/

**Database changes to date: the M7 apply of 2026-09-15** (tw.product_revenue and
tw.product_revenue_status for the 566 manifest companies) **and the post-M7 `db.fill_weight` run of the same day**
(76 exposure rows of 65 companies, exactly the manifest's declared weight-source changes; no other row moved).
The five 'pre-existing weight discrepancies' flagged in M6 were a rounding false positive (erratum:
tmp/rejudge/p22r5/post_m7/). **Then the M5 round 1 apply** (9 companies' evidence and 15 exposure rows,
transaction 801960) and its db.fill_weight run. v3 HOLD / REJECT, and 6582 / 8027, untouched.

**Evidence has to earn its way into the database before anyone asks whether it may
change a classification — never the other way round.** Up to M4 this document governs
the inputs to classification; from M5 on the question is classification itself, under
the rules ruled 2026-09-02. M5 does not reopen the parser unless the classification
evidence itself exposes a reader regression; otherwise the causal boundary blurs again.

**A write is not one kind of thing.** The 421 writes on v2 are three. That count is
provisional: P22 (a prior-year value that is item-for-item and sum-for-sum
indistinguishable from a refresh) may remove some of them, and it is not a baseline
until the P22 gate in the M5 section closes.

    415  AUTO_REFRESH                    evidence refresh: a missing value supplied
      4  PERIOD_SCOPE_REPLACEMENT        replacement: the stored value was another period's
      2  INVALIDATED_EXISTING_EVIDENCE   replacement: the stored value failed its own arithmetic

All three are legitimate. Only the first is a refresh, and an audit that calls the
other six "updates" loses the reason they were allowed.

**A reader regression is independent of the reconciliation result.** 2332's regression
surfaces as a rejection because the stored value is right; 9924's as `NO_CHANGE`
because the stored value happens to match the regressed read. "The database did not
change" is not evidence that the release is sound.

---

## Five phases

The first phase that settles a company settles it.

| phase | question | actions |
|---|---|---|
| **(1) producer ownership** | may this reader touch the stored evidence at all? | `OWNERSHIP_PRESERVED` · `HOLD_PRODUCER_CONFLICT` |
| **(2) locks** | is a prior retraction, or a boundary this release was kept out of, in force? | `HOLD_ADJUDICATION` · `HOLD_SCOPE` |
| **(3) evidence reconciliation** | is the proposal admissible, and what kind of change is it? | `HOLD_UNVERIFIED` · `HOLD_SOURCE_UNAVAILABLE` · `HOLD_EVIDENCE_CONFLICT` · `REJECT_SEMANTIC` · `HOLD_LOST` · `HOLD_ROWS_CHANGED` · `HOLD_EVIDENCE_RECONCILIATION` · `HOLD_VALUE_CONFLICT` · `VALUE_EQUIVALENT_ROUNDING` · `NO_CHANGE` |
| **(4) classification** | does a change the reader may make move a classification? | `REQUIRES_READJUDICATION` |
| **(5) persistence** | only then: write | `AUTO_REFRESH` · `INVALIDATED_EXISTING_EVIDENCE` · `PERIOD_SCOPE_REPLACEMENT` · `STATUS_RECONCILIATION` |

The order replaces an implicit one that ran *candidate → looks newer → overwrite*.

**Evidence runs before classification on purpose.** A company whose rows changed
*and* whose cited weight moved has to have its rows settled first; the classification
question is only meaningful once the evidence under it stands. It leaves phase (3)
as an evidence hold, and its detail carries `classification also affected -- …` so
the classification impact is not lost on the way.

**Exactly one shape is written automatically:** the same item, a stored value that
was missing, a new value supplied by disclosed evidence. Where both sides already hold
a number and the numbers differ, the reader reading a different value is not, by
itself, authority to replace the stored one. For the automatic class, 89 cited rows
on v1 agree with weights derived independently from amounts, to a median difference
of 0.004 points. A sum reaching 100 is not such evidence — an income statement and a
market-share table reach 100 too.

### Tests within the phases

| phase | action | test |
|---|---|---|
| 1 | `OWNERSHIP_PRESERVED` | another producer owns the live rows, and the reader proposes nothing or its material values agree with the owner's (O2) |
| 1 | `HOLD_PRODUCER_CONFLICT` | another producer owns the live rows and the reader's material values disagree |
| 2 | `HOLD_ADJUDICATION` | deliberately retracted and parked at `no_section`, and the reader reads it again |
| 2 | `HOLD_SCOPE` | inside (4) W2-01/W3, (5) financial, or (6) the Gate 1 product/region boundary |
| 3 | `HOLD_UNVERIFIED` | a rescue: rows proposed where nothing was stored |
| 3 | `HOLD_SOURCE_UNAVAILABLE` | empty candidate over stored rows: `download_failed`, `no_report`, `parse_failed` |
| 3 | `HOLD_EVIDENCE_CONFLICT` | empty candidate over stored rows: `parse_conflict` — the source was read and contradicts itself |
| 3 | `REJECT_SEMANTIC` | not admissible as a mix (caption as item, geography axis, coherent mix replaced by incoherent one), or a disclosed percentage would become NULL |
| 3 | `HOLD_LOST` | `no_section` over rows the reader wrote: found nothing where it once found something |
| 3 | `HOLD_ROWS_CHANGED` | the item set gained or lost rows |
| 3 | `HOLD_EVIDENCE_RECONCILIATION` | the item set was replaced outright and no classification depends on it |
| 3 | `VALUE_EQUIVALENT_ROUNDING` | same items; every difference falls within the rounding the filing's printed precision allows (integer ±0.5, one decimal ±0.05, two decimals ±0.005). Not a replacement; nothing written |
| 3 | `HOLD_VALUE_CONFLICT` | same items; a disclosed value would be replaced by a different disclosed value, and neither branch below applies |
| 3 | `NO_CHANGE` | nothing differs |
| 4 | `REQUIRES_READJUDICATION` | the item set under a classification was replaced, or a cited weight leaves the read or moves by more than 1.0 point (resolved exactly as `db/fill_weight.py` resolves it) |
| 5 | `AUTO_REFRESH` | same items; at least one value the store lacked is filled, and no stored value is replaced |
| 5 | `INVALIDATED_EXISTING_EVIDENCE` | same items; the stored set fails its own arithmetic, the reader's passes against a declared total, and every differing stored value is found printed in the reader's own table — the stored read took the wrong column of the same table. A replacement, never an `AUTO_REFRESH`, and its detail is the audit reason |
| 5 | `PERIOD_SCOPE_REPLACEMENT` | same items; both sets complete; the stored values are another reporting period's column of the reader's own table, and the reader's is the only share column labelled with the target period (G1–G5 below). A replacement, never an `AUTO_REFRESH` |
| 5 | `STATUS_RECONCILIATION` | rows unchanged; only the status differs. Not an evidence refresh, and not necessarily a hold |

---

## Closure matrix

Every held state has to answer: **what new evidence would end it?** A hold that cannot
answer is not a hold, it is a place things go to be forgotten.

**mechanical** — a rule decides the company cannot be written, and looking harder does
not change the rule. **semantic** — whether the evidence is right has to be judged
against the filing. **historical** — a prior deliberate decision stands until it is
explicitly revisited.

v1 counts (pre-fix tree; they will move on v2). *exp* = companies carrying a live
exposure. *cls* = companies whose detail notes a classification effect behind the hold.

| phase | action | v1 | exp | cls | class | released automatically? | what ends it |
|---|---|---:|---:|---:|---|---|---|
| 1 | `HOLD_PRODUCER_CONFLICT` | 11 | 11 | — | mechanical | no | a company-level ownership ruling (O3). All 11 are `narrative_text` repairs; 10 cite an owned item and 7 would move a weight on transfer |
| 1 | `OWNERSHIP_PRESERVED` | 130 | 97 | — | — | — | **not a hold.** 33 where the reader proposes nothing, 97 where its material values agree (O2). Nothing is pending. Re-enters only if a later read disagrees |
| 2 | `HOLD_SCOPE` · (5) 金融保險業 label | 38 | 0 | — | mechanical | no | the deferred ruling: G20 industry axis and R020② subsidiary scope |
| 2 | `HOLD_SCOPE` · (5) emerging board, listed by hand | 9 | 0 | — | mechanical | no | as above; the emerging board also lacks any authoritative industry label |
| 2 | `HOLD_SCOPE` · (6) | 2 | 0 | — | mechanical | no | the Gate 1 product/region boundary is ruled (1442, 2545) |
| 2 | `HOLD_ADJUDICATION` | 4 | 0 | — | historical | no | explicit re-adjudication confirming or revoking the prior retraction (e.g. migration 110 for 2884) |
| 3 | `HOLD_SOURCE_UNAVAILABLE` | 0 | — | — | mechanical | **yes, once the source is** | the same filing is fetched, a filing appears, or the document opens |
| 3 | `HOLD_EVIDENCE_CONFLICT` | 1 | 0 | — | semantic | no | the conflict is explained: the parser represents segment × item (P13), or the conflict is shown to be noise |
| 3 | `HOLD_UNVERIFIED` | 10 | 1 | — | semantic | no | independent confirmation that the table is the company's own mix — a second source agrees, or the page is confirmed not a peer, capex or year table |
| 3 | `REJECT_SEMANTIC` | 6 | 0 | — | semantic | — | terminal for this candidate; stored evidence stands. Re-enters with a later admissible read (4545: when P9 is fixed) |
| 3 | `HOLD_LOST` | 9 | 0 | — | semantic | no | the cached filing is checked: no mix → contradiction confirmed, retraction allowed; a mix the reader missed → regression, rows stand, parser defect opened |
| 3 | `HOLD_ROWS_CHANGED` | 39 | 5 | 1 | semantic | no | each added or removed row given its reason: fragment, caption or unit line dropped, or missing line recovered → accept; real line lost or garbage added → reject |
| 3 | `HOLD_EVIDENCE_RECONCILIATION` | 29 | 0 | — | semantic | no | the replacement read is confirmed against the filing as the company's mix, or rejected |
| 3 | `HOLD_VALUE_CONFLICT` | **0 (v2)** | — | — | semantic | no | all nine resolved by the source's own labels — see *Value conflicts, read at the source*. **ruled 2026-09-14:** held by default, released only by **(A)** a formal, source-level precedence rule — never one assigned to a single company — or **(B)** independent corroboration from a source that is not the same PDF. **Neither path exists today:** no precedence rule is written, and `tw.conference_product_mix` holds one company, none of these nine |
| 4 | `REQUIRES_READJUDICATION` | 5 (v1) · 7 (v2) | 5 · 7 | 4 · 6 | classification | no | R023/R024 re-adjudication of the exposure against the new item set |
| 5 | `STATUS_RECONCILIATION` | 2 | 0 | — | — | — | not a hold, not an evidence write: 2388 and 2548, whose rows are identical and whose status only flips between two non-`ok` states |

**Where classification actually moves.** Eight companies: the 5 in phase (4), plus
6533 (behind `HOLD_ROWS_CHANGED`) and 4754 and 6725 (behind `HOLD_VALUE_CONFLICT`).
The ownership queue that looked like the bottleneck — 108 companies, 96 with
exposures — resolved to 11 once agreement between producers was measured: 97 of the
108 would have changed no value at all.

---

## Value conflicts, read at the source

`HOLD_VALUE_CONFLICT` meant two disclosed numbers that both look plausible. The nine
on v2 were read back from the cached filing, cell by cell
(`tmp/rejudge/value_conflict_probe.py`), and every stored value was found printed in
the reader's own table. **None of the nine was a true conflict.** Each has a stated
reason the source itself supplies:

| company | stored value is printed as | reader's is | action |
|---|---|---|---|
| 3629 地心引力 | 113年度 **銷貨金額** 61 — an amount | 114年度 比例% 100.00 | `INVALIDATED_EXISTING_EVIDENCE` |
| 5310 天剛 | 線上遊戲收入 **金額** 24 beside a 97 share; sum 121 | share column 97 / 3 | `INVALIDATED_EXISTING_EVIDENCE` |
| 7883 饗賓 | 113年度 比重(%) | 114年度 比重(%) | `PERIOD_SCOPE_REPLACEMENT` |
| 2066 世德 | 113年度 | 114年度 | `PERIOD_SCOPE_REPLACEMENT` |
| 2465 麗臺 | 113年度 | 114年度 | `PERIOD_SCOPE_REPLACEMENT` |
| 2509 全坤建 | 113年 | 114年 | `PERIOD_SCOPE_REPLACEMENT` |
| 4754 國碳科 | 113度 | 114度 | `REQUIRES_READJUDICATION` — period replacement that moves cited weight J6 (O5) |
| 6725 矽科宏晟 | **112年度** | 114年度 | `REQUIRES_READJUDICATION` — period replacement that moves cited weight A7-01 (O5) |
| 2332 友訊 | **合併公司** 114年度 | **本公司** 114年度 | `REJECT_SEMANTIC` — the reader is wrong (P20); the consolidated evidence stands |

Three replacement grounds, kept apart because they say different things about the
stored value:

    INVALIDATED_EXISTING_EVIDENCE   the stored set fails its own arithmetic
    PERIOD_SCOPE_REPLACEMENT        the stored set is valid, for the wrong period
    VALUE_CONFLICT                  both valid, same scope, same period -> held

**Guards for `PERIOD_SCOPE_REPLACEMENT`** (ruled 2026-09-14): G1 the same source
document; G2 item identity corresponds; G3 each set passes its own completeness check;
G4 the source's column headers name different reporting periods; G5 exactly one share
column carries the target period, and it is the reader's. G5 counts share columns
only — 7883 prints an amount and a share column under 114年度, and an amount column is
not a second target-period share.

**Rounding was zero.** 7883 looked like rounding — a largest difference of 0.13 — but
its table prints two decimals, so ±0.005 is the tolerance and every row exceeds it.
The difference is a year.

**The evidence is per company.** These branches read the printed table, so they reach
only companies whose table has been probed. A value conflict without that evidence
stays `HOLD_VALUE_CONFLICT`.

**The definition of an evidence refresh, after this:** `AUTO_REFRESH` is the same item,
in the same entity scope, the same consolidation scope and the same reporting period,
where the stored value was missing and the new value is directly disclosed. A change of
any of those scopes is not a refresh.

---

## Ownership rules

`tw.product_revenue` has three writers. These rules decide which of them may change
a company's evidence. They are written to be audited like `tw.classification_rules`:
each has an id, a status, and the evidence behind it.

| id | rule | status | evidence |
|---|---|---|---|
| **O1** | A reader never deletes or retracts rows another producer wrote. | **ruled, enforced** | P11/P12: `save()` scoped to `READER_METHODS`; pinned by `tests/test_revenue_row_lifecycle.py` |
| **O2** | Where the reader's material values agree with the owner's, ownership is preserved and nothing is written. Agreement is recorded as corroboration. **Revenue clause (ruled 2026-09-15, v3):** material agreement is judged on pct exactly as before, and on every revenue the owner asserts. An owner's NULL revenue asserts nothing: a reader that matches item/pct and only adds revenue agrees ("owner corroborates item/pct; revenue unasserted by owner"). An owner revenue that the reader contradicts (123 against 456) is a producer conflict. The clause widens nothing on pct. | **ruled 2026-09-14**; revenue clause 2026-09-15 | 97 of 108 overriding reads on v1 changed no value; 14 of them differed only in label spelling, and transferring those would have broken 6 exact `weight_item` keys. v3: 79 narrative-owned companies whose owners store no revenue return to `OWNERSHIP_PRESERVED` |
| **O3** | Agreement never authorises a transfer. Where the reader's values disagree, the owner stands until a company-level ruling says otherwise. | **ruled** ("reader correctness is not write authority") | a candidate describes a document; ownership is a decision about who may write |
| **O4** | A repaired owner is not displaced by the reader reading the company again. The reader regains ownership only when **the named repair reason is shown resolved** *and* its new read is independently valid. The repair reason must be a named, traceable provenance state (`no_stored_table`, `partial_parse`, `region_as_product`, …), never "the parser used to be worse". | **ruled 2026-09-14** | all 11 remaining conflicts are `narrative_text`; in several the reader is plainly worse (6531 reads a balance sheet, 4303 keeps one line summing to 52). See *Repair provenance* below — for 8 of the 11 the reason is no longer recoverable |
| **O5** | If reconciling evidence would change a `weight_item`'s identity, a node attribution, or the weight a classification can consume, the mutation is not an evidence refresh. It enters the adjudication bound to its classification consequence. | **ruled 2026-09-14** | 7 of the 11 would move a weight; `db/fill_weight.py` resolves weights by exact string, so a relabel alone breaks the key. Worded on identity and consumable weight, so a pure value update that leaves both intact is not caught |

The rules separate two questions that the old `save()` answered with one DELETE:

    is this read credible?          agreement, sums, the cached filing
    may this read replace that one? O1-O5

---

## Repair provenance (O4)

O4 needs the reason an owner was repaired. That reason was never stored.
`narrative_text` wrote only `N rows re-read from the narrative text`, its run report
(`tmp/mix_text_report.txt`) is overwritten on every run, and the reader rows it
repaired were deleted outright — so the flag cannot be recomputed either.

What survives is a set of diagnostics from 2026-09-03 (`tmp/_mix*.txt`). Against the
11 conflicts on v1:

| company | repair reason | recoverable |
|---|---|---|
| 2344 華邦電 | `no_stored_table` — mix table in the chapter, 0 rows stored | yes |
| 5487 通泰 | `no_stored_table` | yes |
| 4303 信立 | `partial_parse` — stored total 512,159 against the filing's 988,134 | yes |
| 1307 1708 1723 2101 4305 4722 4767 6531 | — | **no** |

Two consequences. For those eight, O4 cannot be met, so the owner stands until the
reason is re-established by reading the filing. And two of the three recoverable
cases were gap fills, not repairs of broken output — the reader had stored nothing.
**Going forward, any producer that takes ownership records its `repair_reason` at
write time**; without it, a later version cannot know which problem it would be
claiming to have solved.

---

## M5 · classification-impact rulings (2026-09-14)

Eight companies reached this queue. Each was opened at the source table before it was
judged, and three of the eight turned out not to be classification questions.

**Left the queue — the read is rejected, the stored evidence stands**

| id | why |
|---|---|
| 3633 云光 | page 47 is 主要供應商資料, 占全年度進貨淨額比率 — purchases by supplier (R020 ①-C); the Gate 1 label-column defect, live |
| 2404 漢唐 | header 一一三年度 \| 一一四年度; the line reader reads the 113 column (P22). The 2026-09-09 Too_Coarse ruling also forbids filling its weight |

**Status: proposals, not write authority.** M5 is paused at "classification proposals
complete". Nothing below may be applied until the gates at the end of this section close.
`db/fill_weight.py` resolves `weight_item` by exact string against `tw.product_revenue`,
so when these exposure edits are applied they land in the same transaction as the
evidence rows they cite. Editing `db/industry_data.py` first would break every key below.

| id | kind | proposed exposure | stored basis being replaced | open |
|---|---|---|---|---|
| 5315 光聯 | key maintenance | E2-01 Core `液晶顯示器模組(LCM)` 96.79 | same line, key without brackets (96.791 from amounts) | — |
| 4754 國碳科 | A | J6 Core `防火塗料` 48.88 · J4-01 Minor `阻燃劑` 9.77 | 113 column (52.20 · 13.06); Core reason ⑤ now 5× instead of 4×, same direction | — |
| 6725 矽科宏晟 | A, R019 | A7-01 Core `高科技產業製程供應系統設備及相關系統 + 高科技產業製程廠務系統整合工程` 92.70 | 112 column, single line (74.10); 維護及保養 7.30 is not in A7-01's list and stays out | — |
| 6533 晶心科 | C, R019 | A4-06 Core `矽智財授權收入 + 權利金收入 + 矽智財維護服務收入` 100 | 80.729 = 965,403 ÷ (1,478,187 − 282,337): the denominator lost the split 權利金 row | explicit re-adjudication |
| 6582 申豐 | B, R019 | `丁苯乳膠 綠色環保建材` 48.68 → J3 (XSBR, own evidence); `丁腈乳膠` 51.19 **unresolved**, so J3 Core and its aggregate wait | a category label carrying 丁腈's value only (set summed 51.32) | whether J3 covers NBR |
| 8027 鈦昇 | B | laser `雷射設備` 48.81 **unresolved**; plasma `電漿設備` 10.75 **unresolved** (R020 ② + R016) | a category label (半導體設備) carrying 雷射's value only (set summed 89.25) | process role of both lines |

Each exposure's `evidence` text quotes the old numbers or labels and is rewritten in
the same change.

**Reasons, and what each still lacks**

- *R019 aggregation in M5* (ruled). Every row above needs an O5 ruling anyway, and the
  rule says weights on the same leaf are summed. The other companies still cite single
  lines until M7 sweeps them, so for now these six use a different method from the
  rest. The gap is disclosed, not hidden.
- *6533 is not a weight refresh.* It arrived as `HOLD_ROWS_CHANGED`, so it goes
  evidence reconciliation → classification consequence → explicit re-adjudication. It is
  never batched with the weight moves of 4754 / 6725. A4-06's definition names
  授權金、權利金與技術服務費 itself, which is what makes the aggregation a proposal
  worth ruling on.
- *6582: company evidence, not precedent.* A precedent may support a ruling, but it
  cannot stand in for this company's own evidence. The filing (pp. 61–63) calls the
  whole business 合成橡膠乳液, and its first registered activity is C801060
  合成橡膠製造業. **丁苯乳膠** is XSBR latex (羧基丁苯乳膠), made from butadiene and
  styrene. J3 lists SBR, so the company's own evidence places it. **丁腈乳膠** is made
  from butadiene and acrylonitrile, which is NBR latex for gloves. J3 lists only
  「SBR、BR、SBS」. Placing NBR in J3 therefore rests on the reach of the definition
  (2108 南帝 and 6582's stored Core already assume it). That is a definition question to
  rule, not something to infer from the company. **Ruled 2026-09-14: J3 is not widened
  for M5.** 2108's mapping is precedent, not definition text, so 丁腈乳膠 stays unresolved
  until J3's definition is ruled — a product-family concept covering NBR, or strictly the
  three listed rubbers.
- *8027: the industry follows the process role, not the technology's name.* The filing
  lists what each laser machine does: 打印 (chip, wafer backside, glass substrate
  marking), 開槽 (wafer backside, glass substrate), 切割 (SiP IC, sapphire, glass, power
  module, ceramic substrate, PCB) and 鑽孔 (ceramic substrate, IC 封裝基板, ABF 載板,
  glass substrate). Those roles fall in A2-12, A2-13 and B5-01, and 雷射設備 48.81 is
  one line with no split. The 48.81 is not forced into a node to finish M5, so the
  laser line stays unresolved and 8027's Core with it (R016: 雷射設備 is a product name,
  not a unit that decides an industry role). **Plasma is not settled by excluding A2-02.**
  The filing's own use table gives it two roles: 導線架清洗, 先進封裝點膠前清洗 and
  晶圓級封裝電漿加工 (切割, 微蝕刻, 除膠, 側壁蝕刻) are packaging (A2-12), but
  **ABF 載板清洗與活化、改質** is substrate manufacturing, which B5-01 names
  (電漿清洗與表面處理; its boundary with A2-12 is the plant, not the machine). 10.75 is
  not split either, so the plasma line has the same shape as the laser line.
  **Ruled 2026-09-14: both lines unresolved.** That part of plasma's use clearly fits
  A2-12 does not carry the whole 10.75 there, because the same revenue line also
  covers B5-01.

  **Precedent (R020 ② with R016):** use evidence can prove an exposure exists without
  being enough to allocate a consolidated revenue line to a single leaf. A product
  name (雷射設備, 電漿設備) whose documented uses span leaves is not an allocation unit.
  Its revenue stays unallocated until the filing splits it, and a node is never
  chosen because one of its uses fits.

**Not decided in M5**

- **A2-12 / A2-13 overlap on laser processing.** Both definitions name it by
  technology. The boundary has to be written by process role. If it puts laser in
  A2-12, 8027's Core moves to A2-12 at 59.56, and 6207 雷科 has to be re-read.
- **Merged labels as keys.** `丁苯乳膠 綠色環保建材` and `半導體材料 SMD 包裝材料` join
  a product to its application or category column. That is the same Gate 1
  label-column defect. A key built on such a label breaks when the reader is fixed.

**Gates before any of this is written (ordered, 2026-09-14)**

P22 is an **evidence-correctness gate before write authority**, not a classification
gate. It is scanned because the same reader defect can make any legitimate-looking
`AUTO_REFRESH` carry another period's evidence — not because 2404 once sat in the
classification queue (2404 was never in the write set).

1. **P22 full-population scan** (`tmp/rejudge/m5/p22_scan.py`, merged and closed by
   `p22_merge.py`). The line branch is verified on 2404, and the table branch has no
   known positive. The merge asserts company_count = unique_company_count = 2302,
   exactly one record per company, completed_shards = 8, and that the 65 companies
   read before sharding were re-read after it with identical results. Sharding speeds
   up execution; it may not change the population semantics.
2. **P22 affected set, from two scans.** P22-A (fallback-triggered) prints N and its
   tiers, and P22-B (a period recognised, several scopes under it) prints
   `scope_ambiguity ∩ WRITE`, together with the write candidates it could not examine.
   P22 closes for the write set only when **P22-A confirmed_mismatch ∩ WRITE = 0 and
   P22-B scope_ambiguity ∩ WRITE = 0**, each read at the source. A clean P22-A result
   alone says only that the fallback mismatches it found miss the write set. P22-A's
   three states:
   - **CASE 1 · N = 0.** The write set proceeds to M5 write-set finalisation.
   - **CASE 2 · N > 0, intersection = 0.** The write set's authority stands. The defect
     is still fixed and the affected evidence re-read, outside the write set.
   - **CASE 3 · intersection > 0.** The write set shrinks and is recomputed, and the
     affected companies may not enter M6.

   N counts every strict candidate, including a default that happens to land on the
   target period, because that value is right by position rather than by a label the
   reader read. Those are listed separately. In CASE 2 and CASE 3 no affected company
   is a write candidate, the reader is fixed, the affected set is re-read, and
   reconciliation and ownership are recomputed. Fixing 2404 alone and moving on is
   not a closure: P22 is a reader-correctness defect, not a company exception.
3. **M5 write-set finalisation** on the recomputed reconciliation.
4. **M6 · write authority manifest**, immutable, one row per mutation: company · action ·
   old evidence id · new evidence id · producer · reason · classification impact.
   Every `AUTO_REFRESH`, `PERIOD_SCOPE_REPLACEMENT`, `INVALIDATED_EXISTING_EVIDENCE`
   and ruled classification change is a row in it. No `HOLD` and no `REJECT` can be.
5. **M7 · transactional apply** executes the manifest and nothing else. The transaction performs mutations
   already decided; it does not judge.

Until gate 2 closes, **421 is not a final baseline**. The defect P22 describes is the
kind `AUTO_REFRESH` is least able to see: same items, correct sum, plausible values,
wrong period.

---

## Revision history

**2026-09-14, P22 CLOSED.** The whole-filing check of the 120 high-risk companies closed on
the high-risk list. 80 print no other table carrying the same items. The other 40 produced
118 candidates, all read, and only three were another-scope revenue composition: 2013
(個體 taken, already removed), 3432 (合併 and 個體 tables, 合併 taken) and 6803 (the A
consolidated table taken). None was held. Checked against the post-P22 write set of 411,
every closure condition is zero: confirmed period mis-selection, confirmed scope
mis-selection or ambiguity, amount/share scope disagreement, source not located, and
measurement error. Every error found was removed from the write set before closure was
claimed.

**What closure means, and what it does not.** It means that, for the scope-selection risk P22
defines, the specified source verification is complete and the 411 carry no remaining
confirmed P22 error. It does not mean the parser is free of defects: P22-1 to P22-4 are
confirmed parser defects. Closure is the precondition for repairing them, not the end of
parser correctness. 411 is written nowhere. After the repair, the affected companies are
re-read and reconciliation and ownership are recomputed, and the final write set is derived
again from there. No manifest is created before then.

**2026-09-14, P22-C read at the source — the write set shrinks again, 415 → provisional 411.**
P22-C (share and amount scope, plus risk features) closed mechanically: 2302 records,
each with exactly one outcome, and all 415 write candidates checked. Reading at the source
confirmed four more errors in the write set, all ruled `REJECT_SEMANTIC`:
- 2736, 3046 and 6741: the line reader takes the share at the year slot but the revenue
  from the first amount on the line, which is the earliest period (P22-4). 3046's 114
  share also falls back to 113 because the filing prints 「99,65」.
- 2013: its page prints 個體 and 合併 as two tables, each with one value column, and the
  reader takes the first (P22-3). No column scan can see this; the page-level risk
  feature found it.
Of the 120 high-risk companies, 47 with strong scope words were read, and the rest had
their caption lines read. That establishes only that the source page is right; it does
not exclude another-scope table elsewhere in the filing. Ruled 2026-09-14: that is not
closure evidence. A whole-filing check of the high-risk subset runs before P22 may
close. Across the population, 54 companies carry an amount-scope finding, and 29 of them
are `NO_CHANGE`, meaning their stored revenue already holds the wrong period.

**2026-09-14, P22-B read at the source — the write set shrinks, 421 → provisional 415.**
P22-B (a period recognised, several scopes under it) closed mechanically: 2302 records,
58 ambiguous, 9 of them in the write set. Every one of the 9, and the 38 write candidates
whose chosen column fell outside the target candidates, was read at the source with
`tmp/rejudge/m5/source_view.py`. Four confirmed P22 scope mismatches were in the write
set: 8472 and 9930 (consolidation: 個體 taken where 合併 is printed), 2739 (business: one
hotel's guest mix), and 5310 (period: its share column is 114 while its amount column is
113年同期, which would overwrite a correct stored 795). Two rejections outside P22 were
found in the same reading: 5452, a market-share and volume table (R020 ①-C), and 3623,
whose header labels the share and amount columns the wrong way round. All six are
`REJECT_SEMANTIC`, not holds, because each candidate is proven wrong rather than awaiting
evidence. The other five of the 9 ambiguous and 34 of the 38 were read and are right, so
a classifier candidate is not a confirmed error. Follow-ups are recorded apart from the
rejections and carry no write authority: 2739 and 5452 hold wrong stored evidence (an
evidence correction), and 3623 is cited by E2-01 Core (classification impact, not M6).
**Finding that stored evidence is wrong does not give a wrong reader candidate the right
to fix it.** The write set is provisional: the amount-column scope scan (P22-C) and the
high-risk read of the unambiguous write set are still open.

**2026-09-14, M5 queue ruled — the written set did not change (421 → 421).** Two reads
were opened at the source and rejected, and their stored evidence stands: 3633 is a
supplier-purchase table (R020 ①-C), and 2404 is the prior-year column (P22). 2404 had
first been ruled "weight fill" on my premise that the new read was current-year; the
source showed otherwise, and it was re-ruled. `REJECT_SEMANTIC` 4 → 6 and
`REQUIRES_READJUDICATION` 7 → 5. Every company entering adjudication now carries a
ruled kind, and reconcile asserts none arrives without one. The rulings themselves are
in the M5 section above. P22 was found through classification evidence, which is the
condition under which M5 may reopen reader questions. It is being scoped
population-wide before the manifest (M6), and the parser is not modified.

**2026-09-14, period scope ratified; value conflicts closed.** `PERIOD_SCOPE_REPLACEMENT`
ruled with G1–G5 and implemented (4 written, 2 routed to adjudication by O5); P20
applied as a rejection of the reader's parent-only read (2332). `HOLD_VALUE_CONFLICT`
went from 9 to 0, and no company outside the nine moved. v2 writes 421: 415 refreshes,
4 period replacements, 2 invalidations.

**2026-09-14, value conflicts read at the source.** Two branches ruled and implemented:
`VALUE_EQUIVALENT_ROUNDING` with a tolerance derived from printed precision (zero
cases), and `INVALIDATED_EXISTING_EVIDENCE` with three guards (3629, 5310). The entry
screen moved from 0.005 to the database's numeric scale; the `AUTO_REFRESH` set did
not change. Two findings surfaced and are held pending a ruling: prior-year stored
columns (6) and the reader's parent-only column (2332, P20). The audit text for the
invalidations was checked against the printed tables and corrected where a shifted
header had mislabelled a column.

**2026-09-14, O4 · O5 ratified, value conflict ruled, status split out.** O4 and O5 were
ratified in the wording above. `HOLD_VALUE_CONFLICT` now has release conditions (A)
and (B), neither currently satisfiable. The two status-only companies left
`AUTO_REFRESH` for `STATUS_RECONCILIATION`, so **the written set shrank by ruling,
416 → 414, removing exactly 2388 and 2548 and adding none.**

**2026-09-14, ownership rule O2 — the written set did not change (416 → 416).**
Measuring agreement between producers showed that 97 of the 108 overriding reads
carried the same material values as the owner. They move to `OWNERSHIP_PRESERVED`;
`HOLD_PRODUCER_CONFLICT` falls to 11, and the remaining conflicts match an
independently built ownership matrix exactly. Agreement was judged after label
normalisation (narrative_text truncates at a bracket), and that normalisation is used
only to find where no transfer is needed — never to resolve a weight.

**2026-09-14, semantic revision — the written set did not change (416 → 416, company
for company).** Four labels were corrected because the closure matrix showed them
describing something other than the next necessary step:

- *reader proposes nothing* moved out of `HOLD_PRODUCER_CONFLICT` into
  `OWNERSHIP_PRESERVED`: it was a stable end state sitting in a review queue it could
  never leave.
- *item set replaced* split by whether a classification depends on it: 29 of 30 had no
  exposure, so they are evidence questions (`HOLD_EVIDENCE_RECONCILIATION`); the rest
  remain adjudications.
- `HOLD_SOURCE_UNRESOLVED` split into `HOLD_SOURCE_UNAVAILABLE` (nothing usable was read)
  and `HOLD_EVIDENCE_CONFLICT` (it was read and contradicts itself). Different causes,
  different closures.
- The phase order was made explicit, which moved ownership ahead of the adjudication
  lock and evidence ahead of classification. Three companies relabelled from
  `REQUIRES_READJUDICATION` to evidence holds as a result, each carrying its
  classification note.

**2026-09-14, earlier the same day** — `AUTO_REFRESH` split by epistemic status
(`HOLD_VALUE_CONFLICT`); `HOLD_ROWS_CHANGED`, `HOLD_LOST`, `HOLD_UNVERIFIED` and the
scope (5) supplement ruled; `AUTO_ADD` and `AUTO_REMOVE_OWN` retired as unreachable.

---

## What is not decided here

- **A source-precedence rule** (release path A for `HOLD_VALUE_CONFLICT`). Until one is
  written, and while no independent source covers these companies, value conflicts
  have no release path.
- **Repair provenance for 8 `narrative_text` owners** is lost; O4 holds them until the
  reason is re-established.
- P13, P14, P17, P18 remain open. They were checked for whether they reach an action on
  v1 (P13 did, through 1605, now `HOLD_EVIDENCE_CONFLICT`); none is fixed.
- The emerging board has no industry label in `tw.stocks`. Any rule drawn by industry is
  blind to it and must be supplemented by hand, as (5) was.
- No apply script exists. It will execute the plan and nothing else; in particular it
  must never route a `parse_conflict` company through `save()`, which still retracts on
  that status.
