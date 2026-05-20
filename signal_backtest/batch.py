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
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from db.connection import get_cursor
from backtest.data import load_stock_data
from signal_backtest.engine import (
    run_side_backtest,
    run_side_backtest_tiered,
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


def _process_one_stock(
    sid: str,
    factory_names: list[str],
    start_index: int,
    use_cache: bool = False,
) -> tuple[
    str,
    dict[str, list[Trade]],
    dict[str, list[SideResult]],
    dict[str, list[tuple[str, str]]],
]:
    """Per-stock worker: load data, run all factories. Module-level for pickling.

    Returns ``(sid, trades_by_sig, sides_by_sig, skipped_by_sig)``. Lookups
    use ``SIGNAL_FACTORIES`` dict, not function objects, so the pickled
    arg-tuple stays small (just stock_id + name list).
    """
    from signal_backtest.signal import SIGNAL_FACTORIES

    trades_by_sig: dict[str, list[Trade]] = {n: [] for n in factory_names}
    sides_by_sig: dict[str, list[SideResult]] = {n: [] for n in factory_names}
    skipped_by_sig: dict[str, list[tuple[str, str]]] = {n: [] for n in factory_names}

    try:
        data = load_stock_data(sid, use_cache=use_cache)
    except Exception as e:
        for n in factory_names:
            skipped_by_sig[n].append((sid, f"load: {e}"))
        return sid, trades_by_sig, sides_by_sig, skipped_by_sig

    if data.n < start_index + 1:
        for n in factory_names:
            skipped_by_sig[n].append((sid, f"資料不足 ({data.n} 天)"))
        return sid, trades_by_sig, sides_by_sig, skipped_by_sig

    for name in factory_names:
        factory = SIGNAL_FACTORIES[name]
        try:
            spec: SignalSpec = factory(data)
        except Exception as e:
            skipped_by_sig[name].append((sid, f"signal: {e}"))
            continue

        for side, entry, exit_, defense_rules, floor_period, tiers, init_period, flood_ti in (
            ("long",
             spec.signals.long_entry,
             spec.signals.long_exit,
             spec.long_defense,
             spec.long_floor_period,
             spec.long_tiers,
             spec.long_initial_period,
             spec.long_flood_tight_init),
            ("short",
             spec.signals.short_entry,
             spec.signals.short_exit,
             spec.short_defense,
             spec.short_floor_period,
             spec.short_tiers,
             spec.short_initial_period,
             spec.short_flood_tight_init),
        ):
            try:
                if tiers is not None:
                    result = run_side_backtest_tiered(
                        data, side, tiers,
                        exit_=exit_,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
                    )
                else:
                    result = run_side_backtest(
                        data, side, entry, exit_, defense_rules,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
                    )
            except InsufficientDataError as e:
                skipped_by_sig[name].append((sid, str(e)))
                continue

            sides_by_sig[name].append(result)
            trades_by_sig[name].extend(result.trades)

    return sid, trades_by_sig, sides_by_sig, skipped_by_sig


def _process_one_stock_star(args: tuple) -> tuple:
    """Pool.imap_unordered helper — unpacks the arg tuple."""
    return _process_one_stock(*args)


def run_batch_multi(
    factories: dict[str, SignalFactory],
    output_dir: Path,
    limit: int | None = None,
    stock_ids: list[str] | None = None,
    start_index: int = DEFAULT_START_INDEX,
    workers: int = 1,
    use_cache: bool = False,
) -> dict[str, tuple[Path, Path]]:
    """Multi-signal batch runner — share one StockData instance across signals.

    Saves ~5/6 of load + indicator + score-cache work when running 6 signals
    together vs. invoking the single-signal runner 6 times. Each signal's
    output goes to ``output_dir / <signal_name> /`` (same layout as
    ``run_batch``), so downstream tooling is unchanged.

    When ``workers > 1``, parallelizes across stocks via ``multiprocessing.Pool``
    (each worker independently loads data + runs all factories). Use 4-8 for
    full-universe runs; results are merged in arrival order which is fine
    because aggregation sorts internally.

    Returns ``{name: (trades_path, sides_path)}``.
    """
    if not factories:
        raise ValueError("factories must be non-empty")

    sub_dirs: dict[str, Path] = {}
    for name in factories:
        d = output_dir / name
        d.mkdir(parents=True, exist_ok=True)
        sub_dirs[name] = d

    if stock_ids:
        stocks = fetch_stocks_by_id(stock_ids)
    else:
        stocks = fetch_listed_stocks()
    if limit:
        stocks = stocks[:limit]

    print(
        f"股票池：{len(stocks)} 檔，訊號：{', '.join(factories)}，"
        f"workers={workers}, cache={'on' if use_cache else 'off'}",
        file=sys.stderr,
    )

    trades_by_sig: dict[str, list[Trade]] = {n: [] for n in factories}
    sides_by_sig: dict[str, list[SideResult]] = {n: [] for n in factories}
    skipped_by_sig: dict[str, list[tuple[str, str]]] = {n: [] for n in factories}

    factory_names = list(factories)
    t0 = time.time()

    if workers <= 1:
        results_iter = (
            _process_one_stock(sid, factory_names, start_index, use_cache)
            for sid, _name in stocks
        )
    else:
        args_iter = [
            (sid, factory_names, start_index, use_cache)
            for sid, _name in stocks
        ]
        # chunksize=4: stocks vary in length but per-stock cost is roughly
        # uniform (~1s on profile run); larger chunks reduce IPC overhead but
        # hurt progress-bar smoothness. 4 is a reasonable middle ground.
        pool = Pool(processes=workers)
        results_iter = pool.imap_unordered(_process_one_stock_star, args_iter, chunksize=4)

    try:
        for k, (sid, t_by, s_by, sk_by) in enumerate(results_iter):
            for name in factory_names:
                trades_by_sig[name].extend(t_by[name])
                sides_by_sig[name].extend(s_by[name])
                skipped_by_sig[name].extend(sk_by[name])

            done = k + 1
            if done % 50 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(stocks) - done) / rate if rate > 0 else 0
                total_trades = sum(len(v) for v in trades_by_sig.values())
                print(
                    f"  {done}/{len(stocks)}  "
                    f"{elapsed:.0f}s  "
                    f"trades={total_trades:,}  "
                    f"ETA={eta:.0f}s",
                    file=sys.stderr,
                )
    finally:
        if workers > 1:
            pool.close()
            pool.join()

    elapsed = time.time() - t0
    print(
        f"\n完成：{len(stocks)} 檔，耗時 {elapsed:.0f}s，"
        f"訊號數 {len(factories)}",
        file=sys.stderr,
    )

    out: dict[str, tuple[Path, Path]] = {}
    for name in factories:
        d = sub_dirs[name]
        trades_path = d / "trades.parquet"
        sides_path = d / "sides.parquet"
        _write_trades(trades_by_sig[name], trades_path)
        _write_sides(sides_by_sig[name], sides_path)
        if skipped_by_sig[name]:
            (d / "skipped.txt").write_text(
                "\n".join(f"{sid}\t{reason}" for sid, reason in skipped_by_sig[name]),
                encoding="utf-8",
            )
        n_trades = len(trades_by_sig[name])
        n_skipped = len(skipped_by_sig[name])
        print(
            f"  [{name}] trades={n_trades:,}  skipped={n_skipped}",
            file=sys.stderr,
        )
        out[name] = (trades_path, sides_path)
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

        for side, entry, exit_, defense_rules, floor_period, tiers, init_period, flood_ti in (
            ("long",
             spec.signals.long_entry,
             spec.signals.long_exit,
             spec.long_defense,
             spec.long_floor_period,
             spec.long_tiers,
             spec.long_initial_period,
             spec.long_flood_tight_init),
            ("short",
             spec.signals.short_entry,
             spec.signals.short_exit,
             spec.short_defense,
             spec.short_floor_period,
             spec.short_tiers,
             spec.short_initial_period,
             spec.short_flood_tight_init),
        ):
            try:
                if tiers is not None:
                    result = run_side_backtest_tiered(
                        data, side, tiers,
                        exit_=exit_,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
                    )
                else:
                    result = run_side_backtest(
                        data, side, entry, exit_, defense_rules,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
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
