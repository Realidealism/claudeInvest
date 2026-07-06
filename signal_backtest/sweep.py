"""Unified sweep runner — grid-scan signal parameters across the full universe.

Spec file contract (throwaway file, keep in tmp/):

    NAME   = "sell_flee_rise3"      # topic slug, used for output dir
    SIGNAL = "sell_flee"            # factory signal this sweep belongs to
    GRID   = [(80, 0), (90, -10)]   # one tuple per grid point;
                                    # GRID[0] MUST be the current baseline
    def build(data, *params) -> dict:
        # Called per stock per grid point. Returns kwargs for
        # run_side_backtest (everything except data / start_index),
        # e.g. dict(side="short", entry=..., exit_=...,
        #           defense_rules=[...], floor_period=8)

Usage:

    python -m signal_backtest.sweep tmp/sweep_spec_x.py --workers 16 --cache

Per grid point the summary reports n / win% / PF_net / maxL / avg_net plus a
built-in time split: the pooled trades' entry-date midpoint splits each point
into first/second half PF_net; a point whose total PF_net beats the baseline
(GRID[0]) while either half is < 1.0 is flagged ⚠時間不均 (Rubric 5).

Net convention follows _compare.py: pnl_net = 報酬率 (raw fraction) − COST_PCT,
reported ×100. Outputs land in tmp/sweeps/{NAME}/ (summary.csv +
trades_{i}.parquet per grid point). No SQLite archiving here (tool C, later).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import time
from datetime import date as date_t
from datetime import timedelta
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.data import load_stock_data
from signal_backtest._compare import COST_PCT
from signal_backtest.batch import fetch_listed_stocks
from signal_backtest.engine import (
    DEFAULT_START_INDEX,
    InsufficientDataError,
    run_side_backtest,
)


def _load_spec(spec_path: str):
    """Import a spec file by path and validate the contract."""
    path = Path(spec_path).resolve()
    if not path.is_file():
        raise SystemExit(f"spec 檔不存在: {path}")
    spec = importlib.util.spec_from_file_location(f"_sweep_spec_{path.stem}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    missing = [a for a in ("NAME", "SIGNAL", "GRID", "build") if not hasattr(mod, a)]
    if missing:
        raise SystemExit(f"spec 檔缺少必要欄位: {', '.join(missing)}")
    if not mod.GRID:
        raise SystemExit("GRID 不可為空")
    return mod


def _as_tuple(params) -> tuple:
    return params if isinstance(params, tuple) else (params,)


# Worker-side spec module. Loaded once per worker process via the Pool
# initializer — the build function is never pickled across processes.
_WORKER_SPEC = None


def _init_worker(spec_path: str) -> None:
    global _WORKER_SPEC
    _WORKER_SPEC = _load_spec(spec_path)


def _process_one_stock(args: tuple[str, str, bool]) -> dict[int, list[dict]]:
    """Load one stock once, run every grid point. Returns {grid_idx: rows}."""
    sid, name, use_cache = args
    mod = _WORKER_SPEC
    out: dict[int, list[dict]] = {}
    try:
        data = load_stock_data(sid, use_cache=use_cache)
    except Exception:
        return out
    if data.n < DEFAULT_START_INDEX + 1:
        return out

    for gi, params in enumerate(mod.GRID):
        kwargs = mod.build(data, *_as_tuple(params))
        try:
            result = run_side_backtest(
                data, start_index=DEFAULT_START_INDEX, **kwargs
            )
        except InsufficientDataError:
            continue
        if not result.trades:
            continue
        rows = [
            {
                "股票代號": t.stock_id,
                "股票名稱": t.stock_name,
                "方向": t.side,
                "進場日期": t.entry_date,
                "進場價": t.entry_price,
                "出場日期": t.exit_date,
                "出場價": t.exit_price,
                "出場原因": t.exit_reason,
                "持倉天數": t.holding_days,
                "報酬率": t.pnl_pct,
            }
            for t in result.trades
        ]
        out[gi] = rows
    return out


def _pf_net(pnl_raw: np.ndarray) -> float:
    """PF on net pnl, _compare.py convention."""
    net = pnl_raw - COST_PCT
    losses = net[net < 0]
    if not len(losses):
        return float("inf")
    return float(net[net > 0].sum() / abs(losses.sum()))


def _fmt_pf(v: float) -> str:
    return "inf" if np.isinf(v) else f"{v:.4f}"


def _db_float(v) -> float | None:
    """Convert float to None if inf or nan (SQLite cannot store them)."""
    if v is None:
        return None
    try:
        if np.isinf(v) or np.isnan(v):
            return None
    except (TypeError, ValueError):
        pass
    return float(v)


def _archive_to_db(mod, summary_rows: list[dict], out_dir: Path) -> int:
    """Insert one sweep + all grid points into the SQLite DB. Returns sweep_id."""
    from signal_backtest._versions import DB_PATH, SCHEMA

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    try:
        cur = conn.cursor()
        # Resolve baseline version by insertion order (rowid), not text sort.
        cur.execute("SELECT version_id FROM versions ORDER BY rowid DESC LIMIT 1")
        row = cur.fetchone()
        baseline = row["version_id"] if row else None

        param_desc = getattr(mod, "PARAM_DESC", mod.NAME)
        cur.execute(
            "INSERT INTO sweeps (date, signal, name, param_desc, baseline_version,"
            " conclusion, detail_path) VALUES (?,?,?,?,?,NULL,?)",
            (
                str(date_t.today()),
                mod.SIGNAL,
                mod.NAME,
                param_desc,
                baseline,
                str(out_dir),
            ),
        )
        sweep_id = cur.lastrowid

        for r in summary_rows:
            raw = r.get("_params_raw", r["params"])
            params_json = json.dumps(list(raw) if isinstance(raw, tuple) else raw)
            cur.execute(
                "INSERT INTO sweep_points (sweep_id, grid_idx, params, n_trades,"
                " win_pct, pf_net, max_loss, avg_net, pf_net_h1, pf_net_h2,"
                " uneven_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sweep_id,
                    r["grid_idx"],
                    params_json,
                    r["n"],
                    _db_float(r["win_pct"]),
                    _db_float(r["pf_net"]),
                    _db_float(r["max_loss"]),
                    _db_float(r["avg_net"]),
                    _db_float(r["pf_net_h1"]),
                    _db_float(r["pf_net_h2"]),
                    r["uneven_flag"],
                ),
            )
        conn.commit()
        return sweep_id
    finally:
        conn.close()


def run_sweep(
    spec_path: str, workers: int, use_cache: bool, limit: int | None = None,
    no_archive: bool = False,
) -> None:
    mod = _load_spec(spec_path)
    grid = mod.GRID
    stocks = fetch_listed_stocks()
    if limit:
        stocks = stocks[:limit]
    print(
        f"sweep {mod.NAME} (signal={mod.SIGNAL})：股票池 {len(stocks)} 檔，"
        f"grid {len(grid)} 點，workers={workers}, cache={'on' if use_cache else 'off'}",
        file=sys.stderr,
    )

    rows_by_grid: dict[int, list[dict]] = {gi: [] for gi in range(len(grid))}
    args_iter = [(sid, name, use_cache) for sid, name in stocks]

    t0 = time.time()
    if workers <= 1:
        _init_worker(spec_path)
        results_iter = map(_process_one_stock, args_iter)
        pool = None
    else:
        pool = Pool(processes=workers, initializer=_init_worker, initargs=(spec_path,))
        results_iter = pool.imap_unordered(_process_one_stock, args_iter, chunksize=4)

    try:
        for k, res in enumerate(results_iter):
            for gi, rows in res.items():
                rows_by_grid[gi].extend(rows)
            done = k + 1
            if done % 200 == 0:
                elapsed = time.time() - t0
                eta = elapsed / done * (len(stocks) - done)
                total = sum(len(v) for v in rows_by_grid.values())
                print(
                    f"  {done}/{len(stocks)}  {elapsed:.0f}s  "
                    f"trades={total:,}  ETA={eta:.0f}s",
                    file=sys.stderr,
                )
    finally:
        if pool is not None:
            pool.close()
            pool.join()

    wall = time.time() - t0
    print(f"\n完成 {wall:.1f}s", file=sys.stderr)

    # --- time split point: midpoint of pooled entry dates across ALL points
    all_dates = [r["進場日期"] for rows in rows_by_grid.values() for r in rows]
    if not all_dates:
        print("全 grid 零交易，無彙總可報", file=sys.stderr)
        return
    d_min, d_max = min(all_dates), max(all_dates)
    mid_date = d_min + timedelta(days=(d_max - d_min).days // 2)

    # --- per-point stats
    out_dir = Path("tmp") / "sweeps" / mod.NAME
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    baseline_pf = None
    for gi, params in enumerate(grid):
        rows = rows_by_grid[gi]
        df = pd.DataFrame(rows)
        df.to_parquet(out_dir / f"trades_{gi}.parquet", index=False)
        if not rows:
            summary_rows.append(
                dict(grid_idx=gi, params=str(params), _params_raw=params,
                     n=0, win_pct=np.nan,
                     pf_net=np.nan, max_loss=np.nan, avg_net=np.nan,
                     pf_net_h1=np.nan, pf_net_h2=np.nan, uneven_flag=0)
            )
            if gi == 0:
                baseline_pf = np.nan
            continue

        pnl_raw = df["報酬率"].values.astype(np.float64)
        pf = _pf_net(pnl_raw)
        entry_dates = df["進場日期"].values
        h1_mask = np.array([d <= mid_date for d in entry_dates])
        pf_h1 = _pf_net(pnl_raw[h1_mask]) if h1_mask.any() else np.nan
        pf_h2 = _pf_net(pnl_raw[~h1_mask]) if (~h1_mask).any() else np.nan

        if gi == 0:
            baseline_pf = pf
        uneven = int(
            baseline_pf is not None
            and not np.isnan(baseline_pf)
            and pf > baseline_pf
            and (
                (not np.isnan(pf_h1) and pf_h1 < 1.0)
                or (not np.isnan(pf_h2) and pf_h2 < 1.0)
            )
        )
        summary_rows.append(
            dict(
                grid_idx=gi,
                params=str(params),
                _params_raw=params,
                n=len(df),
                win_pct=(pnl_raw > 0).mean() * 100,
                pf_net=pf,
                max_loss=pnl_raw.min() * 100,
                avg_net=(pnl_raw - COST_PCT).mean() * 100,
                pf_net_h1=pf_h1,
                pf_net_h2=pf_h2,
                uneven_flag=uneven,
            )
        )

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(out_dir / "summary.csv", index=False, encoding="utf-8-sig")

    # --- terminal table
    print(f"\n=== sweep {mod.NAME} (signal={mod.SIGNAL}) ===")
    print(f"時間切點（pooled entry_date 中點日）：{mid_date}  "
          f"[{d_min} ~ {d_max}]")
    pw = max(len(str(p)) for p in grid) + 1
    print(
        f"{'#':>3} {'params':<{pw}} {'交易':>6} {'勝率%':>6} {'PF淨':>8} "
        f"{'前半PF':>8} {'後半PF':>8} {'淨均%':>7} {'最大虧%':>8}  flag"
    )
    for r in summary_rows:
        if r["n"] == 0:
            print(f"{r['grid_idx']:>3} {r['params']:<{pw}} {0:>6}  (no trades)")
            continue
        flag = "⚠時間不均" if r["uneven_flag"] else ""
        print(
            f"{r['grid_idx']:>3} {r['params']:<{pw}} {r['n']:>6} "
            f"{r['win_pct']:>6.1f} {_fmt_pf(r['pf_net']):>8} "
            f"{_fmt_pf(r['pf_net_h1']):>8} {_fmt_pf(r['pf_net_h2']):>8} "
            f"{r['avg_net']:>7.3f} {r['max_loss']:>8.1f}  {flag}"
        )
    print(f"\n落檔：{out_dir}  (summary.csv + trades_{{i}}.parquet)")
    print(f"wall time: {wall:.1f}s", file=sys.stderr)

    # --- archive to DB unless suppressed
    if no_archive:
        print("[--no-archive] 已跳過 DB 封存", file=sys.stderr)
    else:
        try:
            sweep_id = _archive_to_db(mod, summary_rows, out_dir)
            print(
                f"\n[DB] sweep 已封存：sweep_id={sweep_id}  "
                f"(data/signal_versions.db 已變更，待 commit)"
            )
        except Exception as exc:
            print(f"\n[DB] 封存失敗（sweep 數值不受影響）：{exc}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description="統一 sweep runner（spec 檔模式）")
    ap.add_argument("spec", help="spec 檔路徑（tmp/sweep_spec_*.py）")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--cache", action="store_true", help="StockData pickle 快取")
    ap.add_argument("--limit", type=int, default=None, help="只跑前 N 檔（試跑用）")
    ap.add_argument("--no-archive", action="store_true",
                    help="跳過 DB 封存（試跑用）")
    args = ap.parse_args()
    run_sweep(args.spec, args.workers, args.cache, args.limit,
              no_archive=args.no_archive)


if __name__ == "__main__":
    main()
