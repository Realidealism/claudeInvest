"""Ad-hoc: rank all active stocks by total long/short score on the latest bar.

Outputs the top 50 long candidates and top 50 short candidates.
Tie-breaker: turnover (成交金額) descending — surface actively-traded names
first when many stocks share the same score (common at score extremes)."""

from __future__ import annotations
import sys

from db.connection import get_cursor
from backtest.data import load_stock_data
from analysis.score import build_scoreboard

# Stock list
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

board = build_scoreboard()
results = []  # (stock_id, name, total_long, total_short, long_pct, short_pct, latest_date, turnover)

for k, (sid, name) in enumerate(stocks):
    if k % 100 == 0:
        print(f"  {k}/{len(stocks)} ...", file=sys.stderr)
    try:
        data = load_stock_data(sid)
    except Exception:
        continue
    if data.n < 60:
        continue
    try:
        result = board.evaluate(data, data.n - 1)
    except Exception:
        continue
    turnover = float(data.turnover[-1]) if data.turnover[-1] == data.turnover[-1] else 0.0
    results.append((sid, name, result.total.long_score, result.total.short_score,
                    result.total.long.pct, result.total.short.pct,
                    data.dates[-1], turnover))

print(f"Scored {len(results)} stocks\n")


def _fmt_turnover(t: float) -> str:
    """Format turnover (TWD) as 億 / 萬."""
    if t >= 1e8:
        return f"{t / 1e8:>6.2f}億"
    return f"{t / 1e4:>6.0f}萬"


# Top 50 long: sort by long pct desc, tie-breaker turnover desc
top_long = sorted(results, key=lambda r: (-r[4], -r[7]))[:50]
print("=" * 78)
print(f"前 50 名多方（{top_long[0][6] if top_long else 'n/a'} 收盤）")
print("=" * 78)
print(f"{'rank':>4s} {'stock':<8s} {'name':<20s} "
      f"{'做多':>6s} {'做空':>6s}  {'成交金額':>9s}")
for i, (sid, name, lo, sh, lp, sp, _d, tv) in enumerate(top_long, 1):
    print(f"{i:>4d} {sid:<8s} {name[:14]:<20s} "
          f"{lp:>+6.1f} {sp:>+6.1f}  {_fmt_turnover(tv):>9s}")

# Top 50 short: sort by short pct desc, tie-breaker turnover desc
top_short = sorted(results, key=lambda r: (-r[5], -r[7]))[:50]
print()
print("=" * 78)
print(f"前 50 名空方")
print("=" * 78)
print(f"{'rank':>4s} {'stock':<8s} {'name':<20s} "
      f"{'做空':>6s} {'做多':>6s}  {'成交金額':>9s}")
for i, (sid, name, lo, sh, lp, sp, _d, tv) in enumerate(top_short, 1):
    print(f"{i:>4d} {sid:<8s} {name[:14]:<20s} "
          f"{sp:>+6.1f} {lp:>+6.1f}  {_fmt_turnover(tv):>9s}")
