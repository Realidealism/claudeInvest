# 4544 春日 / G5 — product specificity does not move a machine tool out of its industry

**A documentation fixture, not an executable test.** The question it records —
which of two true descriptions of a machine is its industry identity — is
settled by reading a definition against a declared axis, which no SQL check
can do.

## How it presented

`G5 工具機` carried a circular boundary:

```
★與 G6 的分界是「是否為工具機」：綜合加工中心機、車床、銑床、磨床屬本節點；
其餘專用機械屬 G6。
```

It defines the node by its own name and works only by enumeration. The 2026-09-08
population sweep found nine metal-cutting machine tool makers it handled fine —
and one company it could not place at all.

## The company that broke it

4544 春日, FY2025: 成型機 77.39% ＋ 輾牙機 5.79% ＋ 打頭機 5.51%. Its products
are Boltformer, Nutformer, Parts former, Mini former, and its own filing says:

> 本公司及其子公司所生產之扣件成型機為生產扣件(Fastener)必備之**工具機**

Two descriptions are simultaneously true of this machine:

| description | reading |
|---|---|
| it forms metal workpieces | a machine tool → `G5` |
| it exists only to make fasteners | a special-purpose machine → `G6` |

An audit had proposed **是否由被服務的製程／產品定義** as G5's axis. Against that
axis 春日 goes to `G6`. Against 加工方式 it goes to `G5`. One company, two
declared axes, opposite answers — which is what says the axis had not been
settled, not that the company was ambiguous.

## The ruling

G5's axis is **the machine's own economic and product identity**, not what it
makes or how dedicated it is. 春日 → `G5`: a machine tool that happens to be
dedicated to fasteners.

The alternative was rejected on where it leads, not on this case alone:

```
一般 CNC 車床              → G5
專門加工某一零件的 CNC      → G6
```

That is not a stable industry boundary. Industry identity must not be broken by
customer or application specificity.

## What the ruling did *not* discard

「由被服務的製程／產品定義」survives, demoted to second position:

> 先判設備本身的 machine identity；只有在該設備不屬既有工具機 identity 時，
> 再看其主要製程、材料或產品用途是否構成 G6 的專用設備。

That ordering is what places 6603 富強鑫 (塑膠射出成型機 94.61%) and 4575 銓寶
(PET 吹瓶機 85.16%) in `G6` — **not** because they serve one product, but
because injection and blow moulding are a different equipment identity from a
metalworking machine tool.

## The precedence rule this established

**機床 identity 優先於 application.** A machine tool stays in `G5` whether its
customer is automotive, aerospace, fasteners or mould making. A machine whose
identity is a named process branch's equipment (PCB drilling/routing,
semiconductor wafer processing, display process) stays with that branch —
B5 / A2 / E7 — and does not enter `G5` merely for also being a cutting machine.
Applied per revenue line, not per company: 4526 東台 carries a PCB 加工機 line of
its own, and 1528 恩德's PCB 鑽孔成型檢測機 was already excluded from its G5
weight under R020②.

## Where the ruling was tested next

The ruling was made on one company, which is the weakest form of a boundary. It
was tested three more times the same day, and the three are not interchangeable.

Two came out of the `G6` audit and are recorded in G6's own definition: 1539 巨庭
(woodworking machines, 「雖極專用仍屬 G5」) and 4562 穎漢 (tube bending, cutting
and end-forming machines). Both **passed through G5 first and so never reached
G6** — which is how G5-before-G6 became part of G6's precedence list. Neither
company is mapped yet.

The third came from inside a company that was being mapped, which makes it a
different kind of evidence: not a candidate held up against the boundary, but a
line already sitting in a member's revenue table.

4526 東台's FY2025 filing carries a line literally named **專用機 1.62%**,
described as:

> 因應零件大量生產功能，以特殊結構、主軸箱及專用機夾治具裝置，可從事鑽孔、攻牙、
> 搪孔、拉孔等功能，主要用於高精密、大批量之零件加工

Everything in that sentence is a machining operation and a spindle box. The word
專用 is doing the same work it did at 春日 — describing how dedicated the machine
is, not what kind of machine it is — so the line was counted into `G5`
(51.43 ＋ 17.63 ＋ 1.62 = 70.68%).

What keeps it from being a restatement of 春日: 春日's dedication is to a
*product* (fasteners), 東台's is to a *production pattern* (one part, high
volume, custom fixturing), and 巨庭's is to a *material* (wood). Three kinds of
specificity, all of which the rejected axis would have routed to `G6` — which is
what says the ruling generalises past its founding case rather than describing
one company.

The same company also demonstrates the other half of the precedence rule in the
same filing: its 印刷電路板加工機 9.84% went to `B5-01`, because PCB drilling and
routing is a named process branch's equipment. **One filing, one round, both
directions**: 專用 did not move a machine tool out of G5, and 加工機 did not pull
a PCB machine into it.

## The shape to recognise

Two adjacent nodes, and the tempting axis is the one that reads off the
*customer*. G3 collapsed because its three leaves described the robot's
deployment rather than the robot; G1 dissolved because 「自動化」 described what
equipment does for a customer rather than what the company delivers. Here the
same pull appeared as 「專用 vs 通用」. The stable question in all three was the
same one: **what is the company delivering, and what is that thing?**
