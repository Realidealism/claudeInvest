# 6215 和椿 / G3 — one revenue line crossing three siblings is a partition failure

**A documentation fixture, not an executable test.** What it records — whether
three sibling cells describe the same classification axis — is answered by
reading three names against one another, which no SQL check can do.

## How it presented

`G3` 機器人 had three leaves and no definition of its own:

```
G3-01  Industrial / Collaborative Robot   工業機器人、協作機器人 (Cobot)
G3-02  Mobile Robot                       AMR、AGV
G3-03  Service / Humanoid Robot           Service Robot、Humanoid
```

All three were empty (n=0), so no membership check could see anything wrong.
The three names look like a clean product taxonomy — which is exactly why the
defect survived.

## The evidence that decided it

6215 和椿, FY2025, one revenue line at **11.10%**, described in the annual
report as:

> 整合自主移動機器人(AMR)、工業機器人與服務型機器人，並代理銷售各類型商用配送、
> 清掃及調酒、廣告機器人

One economic activity. It names `G3-02`, `G3-01` and `G3-03` in a single
sentence. 2049 上銀's 機器人 10.00% line does the same thing from the other
direction: 關節式與史卡拉機械手臂 (industrial), 晶圓移載系統 (semiconductor
equipment), 下肢復健機器人 (medical) all under one item.

## Why that is a defect and not just a coarse filing

The three names are not three values of one variable. They are three different
variables:

```
Industrial / Collaborative   應用場域 ＋ 人機關係
Mobile                       移動型態／平台型態
Service / Humanoid           應用場域 ＋ 外形
```

They can be true at once — an AMR is mobile *and* industrial; a humanoid is
mobile *and* service; a cobot is industrial *and* human-collaborative. Siblings
that can all be true of the same activity do not partition anything, so a
company cannot be placed by asking which one it is.

## The ruling

Collapse to one leaf `G3-01 機器人`. Movement type, collaboration, form factor
and application field become **product attributes** — available as Theme or
descriptive property, never as an Industry partition.

## The shape to recognise

`G4` and `G3` were fixed on the same day in opposite directions:

| | defect | operation |
|---|---|---|
| `G4-01` | one cell holding two functions of its parent's declared axis | **split** |
| `G3` | three cells drawn on three different axes, overlapping | **merge** |

Both follow one higher-order rule: **L2 siblings must share a single
classification axis and form mutually exclusive territory on it.** The
operation is whatever that rule requires — neither splitting nor merging is
the fix by itself.

## What is *not* the argument

Not "the population is too small to justify three cells." Observed population
is evidence strength, never an ontology argument (see
`1560_zhongsha_axis_function_not_population.md`, and
`db/TAXONOMY_GAPS.md`'s header on P1/P2/P3). Had 和椿's line been the only
evidence in the tree it would still have carried this ruling, and had there
been thirty robot makers the three cells would still not have partitioned them.
