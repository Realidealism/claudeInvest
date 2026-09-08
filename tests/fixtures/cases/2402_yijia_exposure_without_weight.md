# 2402 毅嘉 — positive evidence builds an exposure even when revenue will not split

**A documentation fixture, not an executable test.** The decision rests on
reading a filing's product list against a node definition; see the note in
`1595_chuanbao_refurbishment_is_its_own_activity.md` for why that is not
expressed as SQL.

## How it presented

`tw.product_revenue_share` FY2025, two lines:

```
電子整合元件產品(含商品+模具+模組)   80.00%
機構整合元件產品(含商品+模具+模組+散熱模組)   20.00%
```

The Core sat on `F4-01 機構` with the **20.00%** line, and the 80.00% line was
claimed by nothing at all. `B2-02 軟板` held a Major whose evidence was the word
軟板 matching inside a research-and-development paragraph — which is why a
2026-09-06 pass had demoted it from Core for having no support in ①④⑤.

## What the filing actually says

商品項目 describes the larger line by what it is:

> 電子模組：**FPC、PCB、SLP 之佈線設計、測試**，可配合 SMT 製程與其他零組件組裝。

計畫開發之新商品 lists 18 items, of which 10 are flexible-circuit work — 多層軟板、
極細線路軟板、雙面 COF 板、光通訊軟板、軟板基材、高速訊號軟板、無邊框螢幕應用之軟板、
散熱片 (TGP) 軟板 — and one names rigid-flex outright:

> (16) CCM&OLED **軟硬結合板**專案

## The ruling

Core moves to `B2-02 軟板 (FPC)` on ⑤ (80.00% ≫ 20.00%) with ④ and ① agreeing;
`F4-01` keeps 20.00% as Major; `B2-03 軟硬結合` gains a Major. Both B2 rows carry
`weight` NULL: the 80.00% line spans FPC, PCB, SLP and SMT assembly with no split
in the filing (R020②), so neither node may book it and the two must never be
summed as though each owned 80%.

Two nodes named in that same line were deliberately **not** added:

- **`B2-01` 硬板** — 「FPC、PCB、SLP 之佈線設計、測試」 states what the company does
  *to* those boards. Nothing in the filing describes rigid-board or SLP capacity
  of its own, and a product word appearing in a service description is not
  evidence of making the product.
- **`V7-01` EMS** — 「可配合 SMT 製程」 is a capability offered alongside its own
  modules, not a contract-manufacturing line sold on its own. V7 asks for
  EMS/OEM/ODM revenue or a self-description as a contract manufacturer.

## The shape to recognise

- An exposure is a **membership claim, not a revenue claim**. Where a filing
  names an activity but will not quantify it, the right answer is a row with
  `weight` NULL — not dropping the row (which loses a true membership) and not
  inventing a split (which fabricates a number).
- Distinguish the three failure modes, because their remedies differ:
  **cannot be held** by any node → taxonomy problem → R025, new node
  (see 1595 川寶);
  **held but not quantifiable** → filing problem → R020②, weight NULL
  (3490 單井's 「其他(註) 33%」);
  **held and demonstrably present** → mapping problem → add the exposure
  (this case).
- 車用 appears throughout this filing and is not a reason to place anything:
  end market is not product structure (R031). The same clause is why B2-01's own
  definition dropped 車用PCB and AI Server PCB on 2026-09-07.
