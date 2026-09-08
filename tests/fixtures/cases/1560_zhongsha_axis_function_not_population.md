# 1560 中砂 / G4-03 — a split is not a new node, and n=1 is not the argument

**A documentation fixture, not an executable test.** The question it records —
whether one cell is carrying two functions — is answered by reading a
definition against a declared axis, which no SQL check can do.

## How it presented

`G4-01` was named 精密機械零組件與工具 (Precision Components & Tooling) and
defined as:

```
CNC、Tooling、Linear Guide、Ball Screw、Bearing、Gear；
研磨與切削工具（砂輪、磨料、鑽石/cBN 工具、切削刀具）
```

Its only member was 1560 中砂, on 砂輪及刀具製品 23.42% — a grinding wheel and
cutting tool maker. So the cell's name led with 零組件, its population was
entirely 工具, and it also listed CNC, which is a whole machine tool.

## What made it a defect rather than a broad cell

`G4`'s axis had already been declared: **依零組件在工業機械中承擔的功能切分**
(傳動與線性運動／驅動／流體控制／工具). Against that axis, 傳動 and 工具 are
two different functions. A single leaf holding both is not a wide definition —
it is a cell that cannot be tested against the axis its parent declares.

## The ruling

```
G4-01  傳動與線性運動        傳遞、轉換、導引或定位機械運動
G4-02  工業製程閥門與流體控制元件  控制工業製程管線中的製程流體
G4-03  切削與研磨工具        以切削或磨耗去除材料
```

CNC left for `G5` (a machine tool is a whole machine, not a component), Tooling
became `G4-03`, and 中砂 moved there keeping its Major role, its 23.42% weight
and its Core on `A1-02` — a node move is not a role decision (R026).

## The shape to recognise, and the argument that is *not* being made

`G4-03` has one member. That is not what establishes it, and the order matters:

1. the axis was declared and stands on its own,
2. reading the cell against the axis showed two functions merged in one leaf,
3. the population audit then supplied mapping evidence.

**n=1 ⇒ node exists** is not the claim, and would be wrong. The claim is
*axis + boundary correction*, with population arriving last as evidence of
strength rather than of existence. The converse guard from `AA` still holds
alongside it: a parent establishing itself does not establish its children, and
each of these three carries its own boundary argument.

Population size is evidence strength, not an ontology rule — which is why the
same round left 液壓／氣壓動力元件 as a candidate with one company and no cell
(see `db/TAXONOMY_GAPS.md`), while splitting this one. The difference is not the
count. It is that tooling was already inside a cell whose axis said it did not
belong there, and fluid power is not inside anything.
