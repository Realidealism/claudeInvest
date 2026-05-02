"""
Batch runner: iterate all listed stocks, compute signals, run long & short
backtests, dump trades to parquet.

Stock universe: tw.stocks where is_active = TRUE
                AND security_type = 'STOCK'
                AND market IN ('TWSE', 'TPEx').
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

from db.connection import get_cursor
from backtest.data import load_stock_data
from signal_backtest.engine import (
    run_side_backtest,
    InsufficientDataError,
    DEFAULT_START_INDEX,
)
from signal_backtest.signal import SignalSpec, SignalFactory
from signal_backtest.trade import Trade, SideResult


def fetch_listed_stocks() -> list[tuple[str, str]]:
    """Return [(stock_id, name), ...] for active TWSE/TPEx common stocks."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT stock_id, name FROM tw.stocks
            WHERE is_active = TRUE
              AND security_type = 'STOCK'
              AND market IN ('TWSE', 'TPEx')
            ORDER BY stock_id
        """)
        return [(r["stock_id"], r["name"]) for r in cur.fetchall()]


def fetch_stocks_by_id(stock_ids: list[str]) -> list[tuple[str, str]]:
    """Return [(stock_id, name), ...] for a user-specified list of IDs.
    Preserves the order requested; missing IDs are dropped with a warning."""
    if not stock_ids:
        return []
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id, name FROM tw.stocks WHERE stock_id = ANY(%s)",
            (stock_ids,),
        )
        name_map = {r["stock_id"]: r["name"] for r in cur.fetchall()}
    out = []
    for sid in stock_ids:
        if sid in name_map:
            out.append((sid, name_map[sid]))
        else:
            print(f"  ⚠ {sid} 不在 tw.stocks，已略過", file=sys.stderr)
    return out


def run_batch(
    factory: SignalFactory,
    output_dir: Path,
    limit: int | None = None,
    stock_ids: list[str] | None = None,
    start_index: int = DEFAULT_START_INDEX,
) -> tuple[Path, Path]:
    """
    Run signal backtest across all listed stocks.

    Returns (trades_path, side_results_path) — both parquet files.
    Trades are flattened (one row per trade); defense_events are JSON-encoded
    so the trajectory is preserved without exploding rows.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if stock_ids:
        stocks = fetch_stocks_by_id(stock_ids)
    else:
        stocks = fetch_listed_stocks()
    if limit:
        stocks = stocks[:limit]

    print(f"股票池：{len(stocks)} 檔", file=sys.stderr)

    all_trades: list[Trade] = []
    all_sides: list[SideResult] = []
    skipped: list[tuple[str, str]] = []

    t0 = time.time()
    for k, (sid, _name) in enumerate(stocks):
        if k % 50 == 0 and k > 0:
            elapsed = time.time() - t0
            rate = k / elapsed if elapsed > 0 else 0
            eta = (len(stocks) - k) / rate if rate > 0 else 0
            print(
                f"  {k}/{len(stocks)}  "
                f"{elapsed:.0f}s  "
                f"trades={len(all_trades):,}  "
                f"skipped={len(skipped)}  "
                f"ETA={eta:.0f}s",
                file=sys.stderr,
            )
        try:
            data = load_stock_data(sid)
        except Exception as e:
            skipped.append((sid, f"load: {e}"))
            continue

        if data.n < start_index + 1:
            skipped.append((sid, f"資料不足 ({data.n} 天)"))
            continue

        try:
            spec: SignalSpec = factory(data)
        except Exception as e:
            skipped.append((sid, f"signal: {e}"))
            continue

        for side, entry, exit_, defense_rules, floor_period in (
            ("long",
             spec.signals.long_entry,
             spec.signals.long_exit,
             spec.long_defense,
             spec.long_floor_period),
            ("short",
             spec.signals.short_entry,
             spec.signals.short_exit,
             spec.short_defense,
             spec.short_floor_period),
        ):
            try:
                result = run_side_backtest(
                    data, side, entry, exit_, defense_rules,
                    start_index=start_index,
                    floor_period=floor_period,
                )
            except InsufficientDataError as e:
                skipped.append((sid, str(e)))
                continue

            all_sides.append(result)
            all_trades.extend(result.trades)

    elapsed = time.time() - t0
    print(
        f"\n完成：{len(stocks)} 檔，耗時 {elapsed:.0f}s，"
        f"總交易 {len(all_trades):,}，跳過 {len(skipped)}",
        file=sys.stderr,
    )

    trades_path = output_dir / "trades.parquet"
    sides_path = output_dir / "sides.parquet"

    _write_trades(all_trades, trades_path)
    _write_sides(all_sides, sides_path)

    if skipped:
        skipped_path = output_dir / "skipped.txt"
        skipped_path.write_text(
            "\n".join(f"{sid}\t{reason}" for sid, reason in skipped),
            encoding="utf-8",
        )

    return trades_path, sides_path


def _write_trades(trades: list[Trade], path: Path) -> None:
    """One row per trade; defense_events serialized as a list of dicts."""
    rows = []
    for t in trades:
        rows.append({
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
            "防守價變化": [
                {"日期": e.date, "防守價": e.price, "原因": e.reason}
                for e in t.defense_events
            ],
        })
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)


def _write_sides(sides: list[SideResult], path: Path) -> None:
    """One row per (stock, side) — summary header without trade detail."""
    rows = []
    for s in sides:
        rows.append({
            "股票代號": s.stock_id,
            "股票名稱": s.stock_name,
            "方向": s.side,
            "起始日": s.start_date,
            "結束日": s.end_date,
            "交易數": s.n_trades,
        })
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)
