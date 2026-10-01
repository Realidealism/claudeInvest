# J8 — the case that fails R025-GATE, so R031 never runs

**2026-09-11.** The negative half of the pair; the positive half is
`a5_technique_axis_truncates_the_measurement.md`.

## How it was framed, and why that framing was wrong

The tree carried this as a boundary problem, in two places:

> J8 電子化學與 J4 特用化學的邊界目前僅由節點名稱區分，尚未寫成可比較的定義，
> 屬 taxonomy backlog

That description invites the reader to treat J8 as a real leaf whose definition
is merely underwritten, and to reach for R031: two cells, plausibly overlapping
territory, write a boundary. It was chosen deliberately as the second stress
test of R031 for exactly that reason.

## R025-GATE

    J8 電子化學       definition: 「電子級化學品」 (six characters, no children)
    members:          1 — 1722 台肥
    台肥 revenue rows: []            (status no_section)

Zero revenue lines. The gate fails, and R031 has no input.

## What the membership actually rested on

Four occurrences of 電子級 in the filing, none of them a current product line:

- 「持續研發相關生產技術…並透過整合上下游供應鏈與**提升至**電子級產品來擴大銷售市場」
  — an intention, stated in the forward-looking half of the chemicals section.
- 「(3) 電子級化學品**產業概況**及發展趨勢、競爭情形：電子級化學品泛指…」
  — the industry-overview chapter describing the category, not the company.
- the continuation of that same paragraph, and a sentence on global demand growth.

「電子化學」 appears zero times. The company's own upstream-to-downstream diagram
sells 硝酸 / 磺胺酸 / 發煙硫酸 into 電子業 as one of nine customer industries.

This is the third instance of one error: **prose from 產業概況 read as a company
self-statement.** The other two are 4923 力士 (工研院IEK's classification table
taken as the filing's own) and 2434 統懋 (「IDM 是降低營運風險的戰略性武器」, a
general industry proposition, nearly taken as evidence of an in-house fab).

## The ruling

J8's membership was withdrawn and the node removed under R025 — no observable
population, no leaf. No territory was lost: J4-01's definition already ends with
「半導體與光電製程用化學品」.

## What this case must keep proving

Without the gate, R031 would have been run on J4 and J8 and would have reported
**no overlap** — correctly, and uselessly, because there was no revenue line to
test. The conclusion drawn would have been "different axes, coexistence safe",
and J8 would have kept a membership built on an intention.

So the sentence to never write is:

> two definitions do not overlap → the two cells may coexist

An unpopulated cell can never be shown to overlap anything, and therefore always
looks safe. The order is **population → overlap → identity vs attribute**, and
skipping the first step makes the second one lie by omission.
