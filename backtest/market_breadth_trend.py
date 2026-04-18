"""Backtest TAIEX using Market Breadth trend signals with spread-exhaustion
overlay.

Variants per scope (short / medium / long) × (normal / total):
  Entry: trend in (bull, strong_bull)
  Exit:  trend in (bear, strong_bear, bull_exhausting)
Execution: at signal day's close price.
"""

from __future__ import annotations

from dataclasses import dataclass

from db.connection import get_cursor
from analysis.market_breadth import classify_trend_series, TREND_CODE


SCOPES = ["short", "medium", "long"]


@dataclass
class Result:
    signal: str
    total_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    trades: int
    win_rate_pct: float
    final_equity: float


def _max_drawdown(equity: list[float]) -> float:
    peak = equity[0]
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = (v - peak) / peak * 100
        if dd < mdd:
            mdd = dd
    return mdd


def _backtest(dates, closes, trends) -> Result:
    position = 0
    entry_price = 0.0
    equity = 1.0
    eq_curve = [equity]
    trade_pnls: list[float] = []

    for i in range(len(dates)):
        t = trends[i]
        price = closes[i]

        if position == 0 and t in (1, 2):
            position = 1
            entry_price = price
        elif position == 1 and t in (-1, -2, 3):
            ret = price / entry_price
            equity *= ret
            trade_pnls.append(ret - 1)
            position = 0

        mark = equity * (price / entry_price) if position == 1 else equity
        eq_curve.append(mark)

    if position == 1:
        ret = closes[-1] / entry_price
        equity *= ret
        trade_pnls.append(ret - 1)

    total_ret = (equity - 1) * 100
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (equity ** (1 / years) - 1) * 100 if years > 0 else 0.0
    mdd = _max_drawdown(eq_curve)
    wins = sum(1 for p in trade_pnls if p > 0)
    win_rate = wins / len(trade_pnls) * 100 if trade_pnls else 0.0
    return Result("", total_ret, cagr, mdd, len(trade_pnls), win_rate, equity)


def _buy_hold(dates, closes) -> Result:
    equity = closes[-1] / closes[0]
    total_ret = (equity - 1) * 100
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (equity ** (1 / years) - 1) * 100 if years > 0 else 0.0
    mdd = _max_drawdown(closes)
    return Result("buy_hold", total_ret, cagr, mdd, 1, 100.0 if equity > 1 else 0.0, equity)


def _pcts(rows, up_key, dn_key):
    ups, dns = [], []
    for r in rows:
        t = r["total_stocks"] or 1
        ups.append(r[up_key] / t * 100)
        dns.append(r[dn_key] / t * 100)
    return ups, dns


def run():
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT b.trade_date, i.close_price, b.total_stocks,
                   b.short_trend, b.medium_trend, b.long_trend,
                   b.short_up, b.short_down,
                   b.medium_up, b.medium_down,
                   b.long_up, b.long_down,
                   b.short_up_total, b.short_down_total,
                   b.medium_up_total, b.medium_down_total,
                   b.long_up_total, b.long_down_total
            FROM tw.market_breadth b
            JOIN tw.index_prices i
              ON i.trade_date = b.trade_date AND i.index_id = 'TAIEX'
            ORDER BY b.trade_date ASC
            """
        )
        rows = cur.fetchall()

    dates = [r["trade_date"] for r in rows]
    closes = [float(r["close_price"]) for r in rows]
    print(f"Backtest period: {dates[0]} ~ {dates[-1]}  ({len(rows)} days)")

    results = [_buy_hold(dates, closes)]

    for scope in SCOPES:
        trends_normal = [r[f"{scope}_trend"] for r in rows]
        res = _backtest(dates, closes, trends_normal)
        res.signal = f"{scope}_trend (normal)"
        results.append(res)

        ups, dns = _pcts(rows, f"{scope}_up_total", f"{scope}_down_total")
        trends_total = [TREND_CODE[t] for t in classify_trend_series(ups, dns, scope=scope)]
        res = _backtest(dates, closes, trends_total)
        res.signal = f"{scope}_trend (total)"
        results.append(res)

    print(f"\n{'Strategy':<28}{'TotalRet%':>12}{'CAGR%':>10}{'MaxDD%':>10}{'Trades':>8}{'Win%':>8}")
    print("-" * 76)
    for r in results:
        print(f"{r.signal:<28}{r.total_return_pct:>12.2f}{r.cagr_pct:>10.2f}{r.max_drawdown_pct:>10.2f}{r.trades:>8}{r.win_rate_pct:>8.1f}")


if __name__ == "__main__":
    run()
