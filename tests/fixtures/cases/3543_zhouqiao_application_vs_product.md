# 3543 州巧 — an application word is not a value-chain role

**A documentation fixture, not an executable test.** Deciding this case needs a
person to read a filing and tell a product body from the market it is sold into.
No SQL check in `db/check_industry.py` can do it, and writing one that always
passes would be worse than writing none — see the "儀式化驗證" failure mode in
`.claude/ref/05_letter.md`. The purpose here is that a future taxonomy review
recognises the shape before repeating the mistake.

## How it presented

`tw.product_revenue_share` FY2025, largest line:

```
顯示器沖壓零組件   62.00%
家居金屬製品       28.00%
醫療用品            8.00%
```

Every population audit for the E 顯示產業 branch searches revenue items for
面板／顯示器／LCD／TFT. This company matched on 顯示器 and entered the E1
candidate set as one of thirty-four.

## What the filing actually says

年報「公司目前之商品(服務)項目」:

> (1) 顯示器結構件製造
> 為電視、顯示器及筆電等光電產品生產周邊**金屬結構件**，包含前框(Bezel)、
> 背蓋、燈罩及接地板等關鍵零組件。

The other lines are the same activity aimed elsewhere: 金屬傢俱骨架、軌道、
壁掛系統 for home furnishing brands, OEM/ODM wheelchairs and care beds, and
from FY2025 AI server rack parts. The company describes itself as integrating
塑膠射出、壓鑄、鈑金加工、烤漆 — metal and plastic forming.

## The ruling

**F4-01 機構 Core**, on 顯示器沖壓零組件 62.00%.

**Not** E1-01 顯示面板, **not** E2-01 顯示與觸控模組. 顯示器 in that line names
the customer's product, not this company's. Its product body is a stamped metal
part, and its value-chain role is mechanical parts — which is why the same
company also stamps furniture frames and wheelchair parts without changing
industry.

## The shape to recognise

A revenue item of the form `<downstream application> + <product body>` will
match any keyword search built on the application half. The E branch hit this
four times in one round with four different words:

| item | application half | product body | went to |
|---|---|---|---|
| 顯示器沖壓零組件 | 顯示器 | 沖壓零組件 | F4-01 機構 |
| 背光模組用擴散片、反射片 | 背光模組 | 光學膜材 | E2-02 |
| LED背光模組 | — | 背光模組 | E2-02 |
| 顯示器驅動IC | 顯示器 | 驅動IC | A4-12 |

R016 and R023 carry the rule this case is evidence for: 下游應用名稱不覆蓋產品
本體的 value-chain role, and 產品名稱相同或高度相似不得單獨作為 exposure 歸屬
依據. This file is the worked example those clauses point at.
