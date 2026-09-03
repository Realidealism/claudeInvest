"""Everything needed to judge one company's industry classification, on one page.

The checks in db.check_industry ask whether a row contradicts itself — no Core,
two Cores, a share on two nodes, a member on a branch node. None of them can ask
the question the rules actually turn on: is this company on the right node?
R011, R015, R016, R022, R023 and R024 all decide that by reading what the company
sells against what the node is for, and no query settles it.

So this assembles the reading material instead of guessing the answer: the
exposures as stored, every product line the annual report reported (marked with
which exposure claimed it, so an unclaimed line is visible), the 業務範圍 prose
for the filings whose ratio table is too coarse to say anything, and the leaf
nodes of the branches the company sits in. A judgement still has to be made by
someone; this is what they need in front of them.

    python -m db.dossier --stocks 2330,2454
    python -m db.dossier --all --out tmp/rejudge/dossier.md
    python -m db.dossier --leaves --out tmp/rejudge/leaves.md

--leaves prints the whole leaf dictionary once, rather than repeating the
branch list in every company's section: a company on the wrong branch needs the
nodes it is NOT on, and those are the ones its own section cannot show.
"""

import argparse
import sys

from db.connection import get_cursor

FISCAL_YEAR = 2025
# Enough of the chapter to name the product lines, which is all it is read for.
NARRATIVE_CHARS = 3000
# The lines that name no product. Same list as db.check_industry's coverage
# denominator — a line excluded there must be excluded here or the two disagree
# about what "unclaimed" means.
UNCLASSIFIABLE = ('其他', '其它', '合計', '其他營業收入', '租賃收入', '勞務收入',
                  '銷貨收入', '銷貨淨額', '營業收入', '營業收入淨額',
                  '銷貨收入淨額', '總計')


def _targets(cur, stocks, branch, everyone):
    if stocks:
        ids = [s.strip() for s in stocks.split(",") if s.strip()]
        cur.execute("""SELECT stock_id, name FROM tw.stocks WHERE stock_id = ANY(%s)
                       ORDER BY stock_id""", (ids,))
    elif branch:
        cur.execute("""SELECT DISTINCT s.stock_id, s.name
                       FROM tw.company_industry_exposure e
                       JOIN tw.stocks s USING (stock_id)
                       WHERE e.effective_to IS NULL AND e.node_code LIKE %s
                       ORDER BY 1""", (branch + "%",))
    elif everyone:
        cur.execute("""SELECT DISTINCT s.stock_id, s.name
                       FROM tw.company_industry_exposure e
                       JOIN tw.stocks s USING (stock_id)
                       WHERE e.effective_to IS NULL ORDER BY 1""")
    else:
        sys.exit("give --stocks, --branch or --all")
    return [(r["stock_id"], r["name"]) for r in cur.fetchall()]


def _exposures(cur, ids):
    cur.execute("""
        SELECT e.stock_id, e.node_code, t.name AS node_name, t.main_products,
               e.role::text AS role, e.weight_pct, e.weight_item, e.weight_type,
               e.weight_basis, e.weight_source, e.evidence_kind, e.evidence,
               e.exposure_type::text AS exposure_type, e.known_date
        FROM tw.company_industry_exposure e
        JOIN tw.industry_tree t ON t.node_code = e.node_code
        WHERE e.effective_to IS NULL AND e.stock_id = ANY(%s)
        ORDER BY e.stock_id, e.role, e.node_code
    """, (ids,))
    out = {}
    for r in cur.fetchall():
        out.setdefault(r["stock_id"], []).append(dict(r))
    return out


def _revenue(cur, ids):
    cur.execute("""
        SELECT stock_id, item, pct_effective, pct_is_derived
        FROM tw.product_revenue_share
        WHERE fiscal_year = %s AND stock_id = ANY(%s) AND pct_effective IS NOT NULL
        ORDER BY stock_id, pct_effective DESC
    """, (FISCAL_YEAR, ids))
    out = {}
    for r in cur.fetchall():
        out.setdefault(r["stock_id"], []).append(dict(r))
    return out


def _narrative(cur, ids):
    cur.execute("""
        SELECT stock_id, section, pages, body FROM tw.product_narrative
        WHERE fiscal_year = %s AND stock_id = ANY(%s)
    """, (FISCAL_YEAR, ids))
    return {r["stock_id"]: dict(r) for r in cur.fetchall()}


def _official(cur, ids):
    cur.execute("""
        SELECT stock_id, twse_official_industry FROM tw.company_official_classification
        WHERE effective_to IS NULL AND stock_id = ANY(%s)
    """, (ids,))
    return {r["stock_id"]: r["twse_official_industry"] for r in cur.fetchall()}


def leaves(cur):
    """Every node that can legally carry an exposure, by branch (R016)."""
    cur.execute("""
        SELECT t.node_code, t.parent_code, t.name, t.main_products, t.level,
               p.name AS parent_name, root.name AS branch_name,
               (SELECT count(*) FROM tw.company_industry_exposure e
                 WHERE e.node_code = t.node_code AND e.effective_to IS NULL) AS members
        FROM tw.industry_tree t
        LEFT JOIN tw.industry_tree p ON p.node_code = t.parent_code
        LEFT JOIN tw.industry_tree root
               ON root.node_code = substring(t.node_code from 1 for 1)
        WHERE NOT EXISTS (SELECT 1 FROM tw.industry_tree c WHERE c.parent_code = t.node_code)
        ORDER BY t.node_code
    """)
    return [dict(r) for r in cur.fetchall()]


def render_leaves(rows):
    out = ["# 可掛曝險的葉節點（共 %d 個）" % len(rows),
           "",
           "只有葉節點可以掛 exposure（R016）。members = 目前掛在該節點的現行曝險數。",
           "節點不存在時可提議新建，但必須說明為什麼既有節點都不合（R025）。",
           ""]
    branch = None
    for r in rows:
        b = r["node_code"][0]
        if b != branch:
            branch = b
            out.append("")
            out.append("## %s %s" % (b, r["branch_name"] or ""))
        out.append("- `%s` %s — %s  [members %d]"
                   % (r["node_code"], r["name"], r["main_products"] or "（無定義）",
                      r["members"]))
    return "\n".join(out)


def render(stock_id, name, exps, lines, narr, official, leaf_rows):
    out = []
    out.append("=" * 72)
    out.append("## %s %s" % (stock_id, name))
    out.append("官方分類：%s" % (official or "（無）"))
    out.append("")

    out.append("### 現有 exposure（%d）" % len(exps))
    if not exps:
        out.append("（無）")
    claimed = {}
    for e in exps:
        w = "NULL" if e["weight_pct"] is None else "%.2f%%" % e["weight_pct"]
        out.append("- `%s` %s ｜ role=%s ｜ weight=%s ｜ %s"
                   % (e["node_code"], e["node_name"], e["role"], w, e["exposure_type"]))
        out.append("  - 節點定義：%s" % (e["main_products"] or "（無）"))
        out.append("  - weight_item：%s" % (e["weight_item"] or "（無）"))
        if e["weight_pct"] is not None:
            out.append("  - weight 來源：%s / %s / %s"
                       % (e["weight_type"], e["weight_basis"], e["weight_source"]))
        out.append("  - evidence_kind：%s" % e["evidence_kind"])
        out.append("  - evidence：%s" % e["evidence"])
        for part in (e["weight_item"] or "").split(" + "):
            if part.strip():
                claimed.setdefault(part.strip(), []).append(e["node_code"])
    out.append("")

    out.append("### FY%d 年報營業比重（%d 條）" % (FISCAL_YEAR, len(lines)))
    if not lines:
        out.append("（年報無營業比重表；判斷須靠業務範圍敘述）")
    total = unclaimed = 0.0
    for r in lines:
        pct = float(r["pct_effective"])
        skip = r["item"] in UNCLASSIFIABLE or r["item"].startswith("減")
        if not skip:
            total += pct
        tag = "（不可分類）" if skip else (
            "→ " + "、".join(claimed[r["item"]]) if r["item"] in claimed else "**未認領**")
        if not skip and r["item"] not in claimed:
            unclaimed += pct
        out.append("- %-28s %6.2f%%%s  %s"
                   % (r["item"], pct, "(推算)" if r["pct_is_derived"] else "", tag))
    if total:
        out.append("")
        out.append("可分類營收合計 %.1f%%；其中未被任何 exposure 認領 %.1f%%"
                   % (total, unclaimed))
    out.append("")

    out.append("### 業務範圍敘述")
    if narr:
        body = (narr["body"] or "").strip()
        out.append("（%s，頁 %s，全長 %d 字。PDF 多欄版面壓平，句子會交錯，"
                   "讀取產品線名稱用，不要當連貫文章讀）"
                   % (narr["section"], narr["pages"], len(body)))
        out.append("```")
        out.append(body[:NARRATIVE_CHARS])
        if len(body) > NARRATIVE_CHARS:
            out.append("…（截斷）")
        out.append("```")
    else:
        out.append("（未取得）")
    out.append("")

    parents = {r["parent_code"] for r in leaf_rows
               if r["node_code"] in {e["node_code"] for e in exps}}
    same = [r for r in leaf_rows if r["parent_code"] in parents]
    out.append("### 同層可選節點（換到別的分支請查 leaves.md）")
    for r in same:
        mark = " ←目前" if any(e["node_code"] == r["node_code"] for e in exps) else ""
        out.append("- `%s` %s — %s%s"
                   % (r["node_code"], r["name"], r["main_products"] or "（無定義）", mark))
    out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stocks", help="comma-separated stock ids")
    ap.add_argument("--branch", help="node_code prefix, e.g. A4")
    ap.add_argument("--all", action="store_true", help="every classified company")
    ap.add_argument("--leaves", action="store_true", help="print the leaf dictionary and stop")
    ap.add_argument("--out", help="write here instead of stdout (UTF-8)")
    args = ap.parse_args()

    with get_cursor(commit=False) as cur:
        leaf_rows = leaves(cur)
        if args.leaves:
            text = render_leaves(leaf_rows)
        else:
            targets = _targets(cur, args.stocks, args.branch, args.all)
            ids = [t[0] for t in targets]
            exps, lines = _exposures(cur, ids), _revenue(cur, ids)
            narr, official = _narrative(cur, ids), _official(cur, ids)
            parts = [render(sid, nm, exps.get(sid, []), lines.get(sid, []),
                            narr.get(sid), official.get(sid), leaf_rows)
                     for sid, nm in targets]
            text = ("# 分類重判材料：%d 家\n\n" % len(targets)) + "\n".join(parts)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
        print("wrote %s (%d chars)" % (args.out, len(text)))
    else:
        print(text)


if __name__ == "__main__":
    main()
