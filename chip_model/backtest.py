"""Look-ahead-safe forward-return backtest vs the TAIEX benchmark.

No look-ahead: a TDCC snapshot dated `data_date` is published *after* that
Friday, so entry is the first trading day *strictly after* data_date. Forward
returns at +4/+8/+12 weeks only ever read prices on or after the entry date.

最大回撤 is computed on a non-overlapping equity curve: cohorts spaced `h`
weekly snapshots apart, each cohort equal-weighted, chained into a curve.
"""
import bisect
from datetime import timedelta

import pandas as pd

from chip_model.db_access import BENCHMARK_INDEX_ID, load_benchmark, load_prices

HORIZONS_WEEKS = (4, 8, 12)


def _lut(prices: pd.DataFrame) -> dict:
    """stock_id -> (dates_list, closes_list) sorted ascending."""
    lut = {}
    for sid, g in prices.groupby("stock_id"):
        g = g.sort_values("trade_date")
        lut[sid] = (list(g["trade_date"]), [float(x) for x in g["close_price"]])
    return lut


def _close_after(dates, closes, target, strict):
    """First (date, close) with date > target (strict) or >= target (not strict)."""
    i = bisect.bisect_right(dates, target) if strict else bisect.bisect_left(dates, target)
    if i >= len(dates):
        return None, None
    return dates[i], closes[i]


def _equity_curve_mdd(trades: pd.DataFrame, h: int) -> float:
    """Max drawdown of a non-overlapping equal-weight equity curve at horizon h."""
    col = f"ret_{h}w"
    by_date = trades.dropna(subset=[col]).groupby("data_date")[col].mean().sort_index()
    selected = list(by_date.index)[::h]  # snapshots are weekly -> step h = h weeks
    eq, peak, mdd = 1.0, 1.0, 0.0
    for d in selected:
        eq *= 1 + by_date[d]
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1)
    return float(mdd)


def _summarize(trades: pd.DataFrame, horizons, signals: pd.DataFrame) -> dict:
    out = {
        "n_signal_weeks": int(signals["data_date"].nunique()),
        "n_signals": int(len(signals)),
        "n_trades": int(len(trades)),
        "horizons": {},
    }
    for h in horizons:
        r = trades[f"ret_{h}w"].dropna()
        ex = trades[f"excess_{h}w"].dropna()
        out["horizons"][h] = {
            "n": int(len(r)),
            "win_rate": float((r > 0).mean()) if len(r) else None,
            "avg_ret": float(r.mean()) if len(r) else None,
            "median_ret": float(r.median()) if len(r) else None,
            "min_ret": float(r.min()) if len(r) else None,   # 最大虧損
            "max_ret": float(r.max()) if len(r) else None,   # 最大獲利
            "avg_excess": float(ex.mean()) if len(ex) else None,
            "max_drawdown": _equity_curve_mdd(trades, h) if len(r) else None,
        }
    return out


def run_backtest(signals: pd.DataFrame, horizons=HORIZONS_WEEKS,
                 benchmark_id=BENCHMARK_INDEX_ID) -> dict:
    """Return {'trades': DataFrame, 'summary': dict}."""
    empty = {"trades": pd.DataFrame(), "summary": _summarize(pd.DataFrame(
        columns=[f"ret_{h}w" for h in horizons] + [f"excess_{h}w" for h in horizons]
    ), horizons, signals)}
    if signals.empty:
        return empty

    sids = sorted(signals["stock_id"].unique())
    start = signals["data_date"].min()
    end = max(signals["data_date"]) + timedelta(weeks=max(horizons) + 2)

    lut = _lut(load_prices(sids, start, end))
    bench = load_benchmark(benchmark_id, start, end)
    b_dates = list(bench["trade_date"])
    b_close = [float(x) for x in bench["close_price"]]

    records = []
    for row in signals.itertuples(index=False):
        sid, sd = row.stock_id, row.data_date
        if sid not in lut:
            continue
        d_list, c_list = lut[sid]
        entry_date, entry_close = _close_after(d_list, c_list, sd, strict=True)
        if entry_date is None:
            continue
        _, b_entry = _close_after(b_dates, b_close, entry_date, strict=False)

        rec = {"stock_id": sid, "data_date": sd,
               "entry_date": entry_date, "entry_close": entry_close}
        for h in horizons:
            target = entry_date + timedelta(weeks=h)
            _, x_close = _close_after(d_list, c_list, target, strict=False)
            if x_close is None:
                rec[f"ret_{h}w"] = None
                rec[f"excess_{h}w"] = None
                continue
            ret = x_close / entry_close - 1
            _, b_exit = _close_after(b_dates, b_close, target, strict=False)
            bret = (b_exit / b_entry - 1) if (b_entry and b_exit) else None
            rec[f"ret_{h}w"] = ret
            rec[f"excess_{h}w"] = (ret - bret) if bret is not None else None
        records.append(rec)

    trades = pd.DataFrame(records)
    return {"trades": trades, "summary": _summarize(trades, horizons, signals)}
