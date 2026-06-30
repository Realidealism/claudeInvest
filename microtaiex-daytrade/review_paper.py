"""Review accumulated paper (or live) trades: PF / win-rate / net after costs.

Usage:
    python review_paper.py [reports\\paper_trades.csv]

Reads the TradeLog CSV (one row per round-trip) and applies the cost model to
report realistic net, so a multi-week forward test can be checked at a glance.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from backtest.replay import CostModel   # noqa: E402


def main(argv) -> int:
    path = argv[1] if len(argv) > 1 else "reports/paper_trades.csv"
    if not Path(path).exists():
        print(f"no file: {path}")
        return 1
    cost = CostModel()
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            pts = float(r["points"])
            lot = int(r["lot"])
            ep, xp = float(r["entry_price"]), float(r["exit_price"])
            gross = float(r["gross_ntd"])
            c = cost.round_trip_cost(ep, xp, lot)
            rows.append({"side": r["side"], "pts": pts, "gross": gross,
                         "cost": c, "net": gross - c,
                         "entry": r["entry_ts"], "exit": r["exit_ts"]})
    if not rows:
        print("no trades yet")
        return 0

    n = len(rows)
    wins = [x for x in rows if x["pts"] > 0]
    gross = sum(x["gross"] for x in rows)
    costs = sum(x["cost"] for x in rows)
    gw = sum(x["gross"] for x in rows if x["gross"] > 0)
    gl = -sum(x["gross"] for x in rows if x["gross"] < 0)
    pf = gw / gl if gl > 0 else float("inf")

    print(f"file={path}  range={rows[0]['entry'][:10]} ~ {rows[-1]['exit'][:10]}")
    print(f"trades={n}  win={len(wins)/n*100:.0f}%  PF(gross)={pf:.2f}")
    print(f"gross={gross:+.0f}  cost={costs:.0f}  net={gross - costs:+.0f} NT$")
    for side in ("buy", "sell"):
        s = [x for x in rows if x["side"] == side]
        if s:
            sw = len([x for x in s if x["pts"] > 0])
            print(f"  {side:4} n={len(s):4} win={sw/len(s)*100:3.0f}% "
                  f"net={sum(x['net'] for x in s):+.0f}")
    # per-day net (forward-test progression)
    by_day = defaultdict(float)
    for x in rows:
        by_day[x["exit"][:10]] += x["net"]
    print("  daily net:", "  ".join(f"{d[5:]}:{v:+.0f}" for d, v in sorted(by_day.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
