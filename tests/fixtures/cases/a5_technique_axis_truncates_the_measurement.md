# A5 — the case that passes R025-GATE and then fails R031

**2026-09-11.** The positive half of the pair; the negative half is
`j8_no_population_means_r031_never_starts.md`. Read them together: what they
establish is the *order* of the rules, not either ruling on its own.

## What the tree held

    A5 封裝與先進封裝
      A5-01 傳統封裝      0 members     technique axis
      A5-02 先進封裝      3 members     technique axis
      A5-05 封裝測試代工   16 members    business-model axis
      A5-06 晶圓中段製程代工 1 member     business-model axis

The mixed axis was known and recorded on 2026-09-07 — R031 already permits
mixed axes to coexist as siblings and simply excludes them from the
mutual-exclusion test, the way V1–V6 (product categories) coexist with V7 EMS
(a business model). The note even named the doubt: 「兩個維度對『受託提供先進
封裝』這條線並不正交」. It then sat as a deferral for four days, because
"different axes" was accepted as a reason not to compare them.

## R025-GATE

A5-02 had three members, each with revenue lines. Gate passed, so R031 applies.

## R031's test, run on the revenue lines

| company | revenue line | A5-02 (wafer-level packaging) | A5-05 (packaging done for customers) | both? |
|---|---|---|---|---|
| 7873 瑞峰 | 晶圓封裝收入 88.22% | yes | yes | **yes** |
| 3265 台星科 | 晶圓級封裝服務 60.06% | yes | yes | **yes** |
| 3374 精材 | 晶圓級尺寸封裝 50.16% | yes | yes | **yes** |

Three of three. Under the clause added to R031 the same day, the tolerance for
mixed axes stops applying the moment one revenue line satisfies both cells, so
the two could not remain siblings.

## Which axis left, and why it was decided by the population

A5-05 holds 16 companies sharing one identity — work done for customers.
A5-02's three members are that same identity. The case that would be
technique-only is named in A5-02's own definition — a wafer maker packaging its
own output — and it has **zero observed members**. A5-01 never had any.

So the technique cells left the leaf layer. The claim is deliberately narrow:
not that advanced packaging is not an industry, but that *in the observed
population* it forms no territory independent of the OSAT business model. If
that population appears, R025 builds the cell back.

## The finding that makes this more than tidying

A technique cell does not merely overlap a business model. **It cuts the
company's revenue to fit itself.**

    3265 台星科   60.06%  →  99.04%   (測試服務 38.98% had been excluded)
    3374 精材     50.16%  →  98.02%   (測試服務 47.86% had been excluded)

Both companies sell packaging *and* testing, both for hire. Only the packaging
line fits 「先進封裝」, so the testing line — nearly half of 精材 — was not part
of the exposure at all. Choosing the wrong axis is not a presentation problem;
it silently truncates the measurement.

## What this case must keep proving

- A recorded "different axes, not compared" is a **deferral**, not a
  resolution. It needs a condition under which it expires.
- The expiry condition is at the revenue-line level. Comparing the two
  definitions would not have settled it — the definitions had already been
  compared, four days earlier, and the answer was "different axes".
- Precedence rules are not the fix. A precedence only hides the choice; the
  revenue line still belongs to both cells.
