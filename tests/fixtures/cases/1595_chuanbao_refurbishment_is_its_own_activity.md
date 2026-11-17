# 1595 川寶 — a third value proposition is not a wider definition

**A documentation fixture, not an executable test.** Whether two lines of
business are the same economic activity is a judgement made by reading a
filing. No SQL check in `db/check_industry.py` can make it, and one that always
passed would be worse than none — see the 儀式化驗證 failure mode in
`.claude/ref/05_letter.md`. This records the shape so the next review
recognises it.

## How it presented

`tw.product_revenue_share` FY2025 has two lines only, both segment-level:

```
半導體部門    68.97%
PCB部門       31.03%
```

The 68.97% carried one exposure, `A2-03 薄膜沉積`, holding the Core with
`weight` NULL under R020② — the line was known to span more than one node and
had no split to give them.

## What the filing actually says

年報「業務範圍」, on the semiconductor segment:

> 半導體部門：主要從事半導體沉積 PECVD 製程設備**再生**、PEALD 原子層沉積設備、
> 半導體零組件、EFEM 自動化生產製造。

集團產品線 lists them as separate lines, and the 研發成果 table corroborates
each with its own year: 111 「電漿輔助原子層沉積及蝕刻雙功能機台」(A2-03),
111 「多載台晶圓級傳送設備前端模組」(EFEM, A2-09), 111 「電漿射頻偏壓耦合匹配
控制器」and 112 「先進製程表面光學鍍膜零件」(components, A2-10), and the
segment's first named item, 「半導體前段成熟製程再生設備」.

## The ruling

One disclosure line, four leaf exposures, every `weight` NULL (R020②):
`A2-03` PEALD deposition tools, `A2-09` EFEM, `A2-10` key components, and a new
node **`A2-15` 半導體設備再生 / 翻修**. A fifth item, 「設備零件代理銷售」, is a
distributor line and earns no manufacturing node at all (R022).

The new node was created rather than widening a neighbour, because the three
candidates are three different value propositions:

| | A2-11 零件清洗 / 再生 | A2-14 設備工程服務 | A2-15 設備再生 |
|---|---|---|---|
| object | parts taken off a tool | the tool, in the fab | the tool, as an asset |
| activity | cleaning, stripping, recoating | install, move, repair | acquire, restore, resell |
| what is sold | processing | engineers' time | the refurbished machine |

Pressing 整機翻修 into A2-11 would have merged a service business with a resale
business under one node; that is the case R025 exists for — the existing node
cannot hold the activity, as distinct from holding it with a definition written
too narrowly (which is a definition fix, not a new node).

## The shape to recognise

- **n = 1 is not an argument either way.** One company is the population today,
  not the reason the node exists. The reason is that the activity is
  independent, identifiable and repeatably classifiable — so the definition is
  written for the activity and never mentions 川寶, and the second company to do
  this can be placed by reading the definition alone.
- **The converse trap is equally live**: a new word in a filing is not a node.
  What earned this one was the filing naming it as its own product line, the R&D
  table carrying it across years, and both neighbouring nodes explicitly
  excluding it.
- **Roles do not fall out of the Core.** With no weight to divide, R026 leaves
  Direct exposures at their Major default; ranking the other three Minor because
  one of them holds the Core would be inferring role from a decision that was
  itself made without numbers.

## The Core moved, and that is the second half of the case

`A2-03` held the Core on this reasoning: 「本格是該部門年報具名的第一項
（半導體沉積 PECVD 製程設備再生、PEALD 原子層沉積設備）」. Read again after
`A2-15` existed, that sentence argues against itself — the first named item is
設備**再生**, which is now its own node, and what is left in `A2-03` is the
PEALD tool. The rationale did not become weaker; the activity it pointed at
stopped being in that cell.

So the Core moved to `A2-15` on 2026-09-08, and R023 gained the rule the move
required (**R023-R**): after a leaf is added or split, every affected company's
existing Core rationale must be re-verified, and a Core is never kept merely
because it has historically been there. Taxonomy refinement turns one coarse
exposure into new-build / refurbishment / components / field service, and Core
evidence has to be re-projected onto the refined leaves — otherwise the tree
accumulates Cores that were once right and are now stale.

Two guards came with it:

- **Disclosure order is supporting evidence, not a decision rule.** 「列為第一項」
  can support a Core; it cannot decide one. Here it holds only because two
  independent sources agree — the segment's business description and the group
  product line both lead with refurbishment — and because the item is itself an
  identifiable independent activity.
- **⑤ could only reach the segment, not the cell.** 半導體部門 68.97% against
  PCB 31.03% settles which side the Core is on; inside the segment the figure is
  undivided across four leaves, so it cannot rank them. ①③ did that work. A Core
  reason that cites a number which cannot discriminate between the candidates is
  citing it for the wrong step.
