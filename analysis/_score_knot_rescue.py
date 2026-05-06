"""Ad-hoc: compare score distribution with vs without knot rescue.

Runs build_scoreboard() twice over all active stocks at the latest bar:
  1. baseline — knot_config=None
  2. rescue   — KnotRescueConfig() default (Go-mirror)

Outputs distribution stats, tie-cluster analysis at extremes, and per-stock
delta histogram (how many points moved, in which direction).

Usage:
    python -m analysis._score_knot_rescue
"""

from __future__ import annotations
import sys
from collections import Counter

from db.connection import get_cursor
from backtest.data import load_stock_data
from analysis.score import build_scoreboard, KnotRescueConfig, ScoreBoard


def _build_no_rescue() -> ScoreBoard:
    """Build identical scoreboard but with rescue disabled."""
    board = build_scoreboard()
    board.knot_config = None
    return board


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id, name FROM tw.stocks
            WHERE is_active = TRUE
              AND security_type = 'STOCK'
              AND market IN ('TWSE', 'TPEx')
            ORDER BY stock_id
        """)
        stocks = [(r["stock_id"], r["name"]) for r in cur.fetchall()]

    print(f"Loaded {len(stocks)} active stocks", file=sys.stderr)

    base = _build_no_rescue()
    resc = build_scoreboard()  # default-on rescue

    rows = []  # (sid, name, base_long, base_short, resc_long, resc_short)
    for k, (sid, name) in enumerate(stocks):
        if k % 200 == 0:
            print(f"  {k}/{len(stocks)} ...", file=sys.stderr)
        try:
            data = load_stock_data(sid)
        except Exception:
            continue
        if data.n < 60:
            continue
        try:
            i = data.n - 1
            r_b = base.evaluate(data, i)
            r_r = resc.evaluate(data, i)
        except Exception:
            continue
        rows.append((
            sid, name,
            r_b.total.long_score, r_b.total.short_score,
            r_r.total.long_score, r_r.total.short_score,
        ))

    print(f"\nScored {len(rows)} stocks\n")

    bl = [r[2] for r in rows]
    bs = [r[3] for r in rows]
    rl = [r[4] for r in rows]
    rs = [r[5] for r in rows]

    def _stats(label: str, xs: list[float]) -> None:
        xs = sorted(xs)
        n = len(xs)
        mean = sum(xs) / n
        var = sum((x - mean) ** 2 for x in xs) / n
        std = var ** 0.5
        pcts = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
        ps = [xs[max(0, min(n - 1, int(p * n)))] for p in pcts]
        print(f"{label:>14s}  n={n}  mean={mean:+.1f}  std={std:.1f}  "
              f"min={xs[0]:+.0f}  max={xs[-1]:+.0f}")
        print(f"               1%={ps[0]:+.0f}  5%={ps[1]:+.0f}  "
              f"10%={ps[2]:+.0f}  25%={ps[3]:+.0f}  50%={ps[4]:+.0f}  "
              f"75%={ps[5]:+.0f}  90%={ps[6]:+.0f}  95%={ps[7]:+.0f}  "
              f"99%={ps[8]:+.0f}")

    print("=" * 70)
    print("Long-side score distribution")
    print("=" * 70)
    _stats("baseline.long", bl)
    _stats("rescued.long",  rl)

    print()
    print("=" * 70)
    print("Short-side score distribution")
    print("=" * 70)
    _stats("baseline.short", bs)
    _stats("rescued.short",  rs)

    print()
    print("=" * 70)
    print("Tie clusters at extremes (long-side)")
    print("=" * 70)

    def _tie_table(xs: list[float], label: str) -> None:
        c = Counter(round(x) for x in xs)
        sorted_items = sorted(c.items(), key=lambda kv: kv[0])
        print(f"\n{label} — bottom 10 score buckets:")
        print(f"  {'score':>7s}  {'count':>5s}")
        for s, n in sorted_items[:10]:
            print(f"  {s:>+7d}  {n:>5d}")
        print(f"\n{label} — top 10 score buckets:")
        print(f"  {'score':>7s}  {'count':>5s}")
        for s, n in sorted_items[-10:][::-1]:
            print(f"  {s:>+7d}  {n:>5d}")

    _tie_table(bl, "baseline.long")
    _tie_table(rl, "rescued.long")

    print()
    print("=" * 70)
    print("Per-stock rescue delta (rescued.long - baseline.long)")
    print("=" * 70)
    deltas = [r[4] - r[2] for r in rows]
    nz = [d for d in deltas if d != 0]
    print(f"  Stocks affected by rescue: {len(nz)}/{len(rows)} "
          f"({100*len(nz)/len(rows):.1f}%)")
    if nz:
        c = Counter(round(d) for d in nz)
        for d in sorted(c):
            print(f"    Δ={d:>+4d}  {c[d]:>5d} stocks")
        print(f"  Mean Δ on affected: {sum(nz)/len(nz):+.2f}")
        print(f"  Max +Δ: {max(nz):+.0f}   Min Δ (most negative): {min(nz):+.0f}")


if __name__ == "__main__":
    main()
