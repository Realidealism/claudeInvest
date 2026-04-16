"""Compare weakening path configurations on TAIEX backtest.

Runs the market_breadth_trend backtest across multiple SCOPE_PATHS configs
so we can see trade-offs side by side.
"""

from __future__ import annotations

import sys

sys.stdout.reconfigure(encoding="utf-8")

from db.connection import get_cursor
from analysis import market_breadth as mb
from backtest.market_breadth_trend import _backtest, _buy_hold, _pcts


SCOPES = ["short", "medium", "long"]

# (label, scope_paths)
# Each scope_paths: {"short": (use_2d, use_3d, use_5d), "medium": ..., "long": ...}
CONFIGS = [
    ("no_weakening",       {s: (False, False, False) for s in SCOPES}),
    ("unified_2d",         {s: (True,  False, False) for s in SCOPES}),
    ("unified_3d",         {s: (False, True,  False) for s in SCOPES}),
    ("unified_2d|3d",      {s: (True,  True,  False) for s in SCOPES}),
    ("unified_2d|3d|5d",   {s: (True,  True,  True ) for s in SCOPES}),
    ("per_scope_current",  {"short":  (True, False, False),
                             "medium": (True, True,  False),
                             "long":   (True, True,  True )}),
]


def load_rows():
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT b.trade_date, i.close_price, b.total_stocks,
                   b.short_trend, b.medium_trend, b.long_trend,
                   b.short_up_total, b.short_down_total,
                   b.medium_up_total, b.medium_down_total,
                   b.long_up_total, b.long_down_total
            FROM tw.market_breadth b
            JOIN tw.index_prices i
              ON i.trade_date = b.trade_date AND i.index_id = 'TAIEX'
            ORDER BY b.trade_date ASC
            """
        )
        return cur.fetchall()


def run():
    rows = load_rows()
    dates = [r["trade_date"] for r in rows]
    closes = [float(r["close_price"]) for r in rows]
    print(f"Backtest period: {dates[0]} ~ {dates[-1]}  ({len(rows)} days)\n")

    bh = _buy_hold(dates, closes)
    print(f"buy_hold: TotalRet={bh.total_return_pct:.2f}%  CAGR={bh.cagr_pct:.2f}%  MaxDD={bh.max_drawdown_pct:.2f}%\n")

    # Precompute up/down percentage series per scope (total variant)
    scope_pcts = {}
    for scope in SCOPES:
        ups, dns = _pcts(rows, f"{scope}_up_total", f"{scope}_down_total")
        scope_pcts[scope] = (ups, dns)

    orig_paths = dict(mb.SCOPE_PATHS)
    try:
        header = f"{'Config':<22}" + "".join(f"{s+'_total':>14}" for s in SCOPES)
        print(header)
        print("-" * len(header))

        for label, paths in CONFIGS:
            mb.SCOPE_PATHS = paths
            returns = []
            for scope in SCOPES:
                ups, dns = scope_pcts[scope]
                trends = [mb.TREND_CODE[t] for t in mb.classify_trend_series(ups, dns, scope=scope)]
                res = _backtest(dates, closes, trends)
                returns.append(res.total_return_pct)
            row = f"{label:<22}" + "".join(f"{r:>14.2f}" for r in returns)
            print(row)
    finally:
        mb.SCOPE_PATHS = orig_paths


if __name__ == "__main__":
    run()
