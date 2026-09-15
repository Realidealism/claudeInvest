# Reconciliation v3 — questions and rulings

**Status:** drafted 2026-09-14; **every question ruled 2026-09-15** after the fifth differential run
passed. Implementation: `tmp/rejudge/p22r5/reconcile_v3.py` (v2 helpers loaded verbatim from
`tmp/rejudge/reconcile.py`). The question text below is kept as written; the rulings are:

| id | ruling |
|---|---|
| V3-1 | evidence identity is `(item, pct, revenue)`. Revenue NULL → value continues only for K or a source-verified ACCEPT, otherwise `HOLD_UNVERIFIED`; value → another value is `HOLD_VALUE_CONFLICT` unless V3-4 holds; value → NULL is held. **Exception V3-1(1):** where every stored pct is NULL, the stored revenues sum to ~100 and each equals the candidate's pct within 1, the stored field holds shares (measurement misclassified) → `INVALIDATED_EXISTING_EVIDENCE` (33 companies), never `AUTO_REFRESH` |
| V3-2 | `scope_unresolved` → `HOLD_SCOPE_UNRESOLVED`; K is no exception; 2371 carries P29 in its detail |
| V3-3 | the nine P22 source rejections retire (all `K_confirmed`); 3633 and 5452 go through the phases on their current candidate; 3623 stays rejected |
| V3-4 | `READER_REPAIR_REPLACEMENT`, not limited to P22: the stored rows are the pre-repair read, the diff attributes the change to a ruled repair mechanism, the candidate is source-verified (K or ACCEPT), and no ownership / scope / evidence hold precedes it |
| V3-5 | M5 proposals stand where item set and cited weights are unchanged (8027 included); only an item-set, cited-weight, weight_item or rule-input change reopens M5 |
| V3-6 | ownership, scope and the probe are recomputed on the repaired candidate; recomputation never transfers ownership. O2 gains its revenue clause (an owner's NULL revenue asserts nothing), recorded in `db/RECONCILIATION.md` |

v3 outputs `evidence_action` and `write_authorized` separately. Result: write candidate 570 → M5/O5
final pass → **566** (65 weight-source changes within printed precision are not O5; 3229 3555 7750
6937 go to M5; 6229 to M5 from v3). M6 manifest: `tmp/rejudge/p22r5/m6/`.

**What v3 is.** The same decision model (`db/RECONCILIATION.md`: five phases, O1–O5, value
branches) applied to the **repaired** candidate (`tmp/rejudge/p22r/shard_*.jsonl`), against the
same frozen `before_full.json`. 411 is where it starts from, not what it must come out at.

---

## Q1 · Reconciliation cannot see a revenue change today

**Fact.** `reconcile.py` builds both sides as `(item, pct)` pairs (`before_rows` from v2,
`after` from the candidate). Revenue never enters the comparison. v2 did not need it: the
pre-repair reader's revenue was never under question. The repair changes exactly that:

- P22-4 replaces a revenue with the share's own group's (2736: 251,023 → 322,647; 6741:
  1,095,443 → 1,308,874), with the share unchanged;
- D4 leaves revenue NULL where the stored row carries a borrowed amount;
- P22-1 moved 5310's amount from 113年同期 to 114.

Run as is, each of these reads as `NO_CHANGE`, or it rides an `AUTO_REFRESH` that fills a
share while silently rewriting a revenue nobody reviewed.

**Proposal.** Evidence identity in v3 is `(item, pct, revenue)`. A change confined to revenue
is its own branch, never `NO_CHANGE` and never part of a share refresh: the existing
value-branch rules apply to revenue as they do to pct. A stored disclosed revenue replaced by
NULL is held, just as a disclosed pct replaced by NULL is rejected today.

## Q2 · What does `scope_unresolved` become?

**Fact.** `save()` records it without retracting or replacing anything. The repaired reader
refuses because it cannot select a value uniquely from established scope.

| option | effect |
|---|---|
| **(a) `HOLD_SCOPE_UNRESOLVED`, phase 3** — recommended | nothing written; stored evidence stands; its closure question is "which new evidence ends it" (a filing that labels scope, or a human reading that rules the column) |
| (b) reuse `REJECT_SEMANTIC` | wrong kind: a refusal is not proof that the candidate is wrong, it is the absence of a candidate |

**Stored evidence under a refusal.** Where the refused company's stored rows came from the
very defect now refused (the four K refusals: 7907, 2739, 3046, 4116), the stored rows are
suspect, yet a refusal carries no authority to retract them. Proposal: a follow-up registry,
beside `FOLLOWUP`, and never an automatic retraction.

## Q3 · How the P22 source rejections retire

**Fact.** `SOURCE_REJECTED` rejects the *v2 candidate* of 2404, 8472, 9930, 2739, 5310, 2736,
3046, 6741 and 2013. Against the repaired candidate those texts describe a read that no longer
exists.

**Proposal.** An entry retires only **by evidence**: the company is `K_confirmed` in the
differential run. Its repaired candidate then goes through the phases like any other. A refused
K member (2739, 3046) moves to Q2's action.

The non-P22 entries (3633 supplier table, 5452 market-share table, 3623 swapped labels) are
not what the repair targets. Each stays in force only when its repaired candidate still carries
the rejected shape (the same rows as v2). A changed candidate is re-read, not carried over.

## Q4 · The companies whose stored evidence is already the wrong column

**Fact.** These were `NO_CHANGE` in v2 because the stored rows and the defective read agreed:
1110, 2727, 7742, 7907, 8996, 2492, 4131 (P22-A / P22-B), and 29 line-reader companies whose
stored revenue carries another period (P22-C). With the repaired candidate they now differ from
the store. The v2 branches would sort them as `AUTO_REFRESH` (NULL → value) or
`HOLD_VALUE_CONFLICT` (value → value). `PERIOD_SCOPE_REPLACEMENT`'s G1–G5 require the probe
reading of that table.

| option | effect |
|---|---|
| **(a) a new write kind `READER_REPAIR_REPLACEMENT`** — recommended | granted only when **all** hold: the company's change is `K_confirmed`, or `REVIEW`-accepted with a recorded source reading; ownership is the reader's (O1); and the change is confined to the columns P22 re-selected. It is its own kind, never an `AUTO_REFRESH`, and its detail cites the diff verdict |
| (b) run them through the existing value branches | most become `HOLD_VALUE_CONFLICT`, because (A) source precedence and (B) independent corroboration do not exist; correct evidence then stays unwritable, and the source reading already done is wasted |
| (c) hold them all | safe, but the store keeps evidence already proven wrong |

**The probe is stale.** `value_conflict_probe.json` was built from the v2 candidate's tables
and cannot certify v3's replacements. Any v3 value branch that needs a probe rebuilds it from
the repaired candidate.

## Q5 · The M5 proposals rest on v2 values

**Fact.** `READJUDICATION_KIND` (5315, 4754, 6725, 6582, 8027, 6533) and the weights proposed
in `RECONCILIATION.md` §M5 were computed from v2 candidate values.

**Proposal.** Recheck each against its repaired candidate. Where the rows are identical, the
proposal stands. Where they differ, it goes back to M5 review, and the proposal is not
patched in place.

## Q6 · Ownership and scope are recomputed, not inherited

The ownership matrix (O1–O5; 11 producer conflicts) and the `HOLD_SCOPE` exclusions are
recomputed on the repaired candidate. O2 preserves ownership on material agreement, so a repaired
read that now **disagrees** with a narrative owner, where v2 agreed, becomes a producer conflict
and never a transfer. The 141 narrative-stratum companies are compared on the same
`(item, pct, revenue)` identity as Q1.

## Q7 · What v3 must report

- a v2 → v3 action transition matrix, every moved company explained by its diff verdict
  (`K_confirmed`, `REVIEW` accepted, or unchanged candidate);
- the new write set against 411: every addition and removal, with its reason;
- no manifest (M6) until v3 itself is ruled.

---

## Decisions to rule, when v3 starts

| id | question | recommended |
|---|---|---|
| V3-1 | evidence identity includes revenue | yes, `(item, pct, revenue)` |
| V3-2 | action for `scope_unresolved` | `HOLD_SCOPE_UNRESOLVED`; stored rows stand; follow-up registry |
| V3-3 | retiring P22 source rejections | only on `K_confirmed`; non-P22 entries re-validated |
| V3-4 | stored evidence proven wrong by the repair | `READER_REPAIR_REPLACEMENT` under the three conditions |
| V3-5 | M5 proposals | recheck against the repaired candidate; changed ones return to review |
| V3-6 | ownership, scope and probe | recomputed or rebuilt on the repaired candidate |
