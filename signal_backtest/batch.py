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
    HOLDOUT_START,
)
from signal_backtest.signal import SignalSpec, SignalFactory
from signal_backtest.trade import Trade, SideResult

# Written at the top of an output dir that kept the holdout (--include-holdout);
# _versions add refuses such a dir.
HOLDOUT_MARKER = "HOLDOUT_INCLUDED"
# The baseline (v370, ruling 2026-10-21) runs total-return accounting and the
# margin_futures short constraints; the CLI defaults to it. An output dir run
# any other way is marked, and _versions add refuses it:
#   SHORT_CONSTRAINTS holds a mode other than the baseline's,
#   RAW_RETURN a run without total-return accounting,
#   ADJUSTED_PRICES a run on back-adjusted prices (experiment, 2026-10-20).
SHORT_CONSTRAINTS_MARKER = "SHORT_CONSTRAINTS"
SHORT_CONSTRAINT_MODES = ("off", "margin", "margin_futures")
BASELINE_SHORT_CONSTRAINTS = "margin_futures"
RAW_RETURN_MARKER = "RAW_RETURN"
ADJUSTED_PRICES_MARKER = "ADJUSTED_PRICES"


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


def apply_settings(sid: str, data, short_events: list | None, futures_days: set | None,
                   total_return: bool) -> tuple:
    """Per-stock run settings shared by the batch and sweep runners.

    total_return: sets data.return_factor from the raw rows, outside the cache
    (it moves with the event tables, which the cache key does not see); rows
    that differ from the series fail the run, as skipping the stock would drop
    it from this run only.
    Returns the short side's (force_exit, entry_block, cover_exempt): margin
    constraints from short_events, lifted where stock futures were usable
    (futures_days); all None when short_events is None.
    """
    if total_return:
        from backtest.adjust import adjustment
        from backtest.data import _fetch_prices
        rows = _fetch_prices(sid, None, None)
        if [r["trade_date"] for r in rows] != list(data.dates):
            raise RuntimeError(f"{sid}: price rows differ from the loaded series (stale cache?)")
        data.return_factor = adjustment(sid, rows)[0]
    if short_events is None:
        return None, None, None
    from signal_backtest.short_constraints import futures_exempt, masks
    force_exit, entry_block = masks(data.dates, short_events)
    exempt = None
    if futures_days is not None:
        exempt = futures_exempt(data.dates, futures_days)
        entry_block = entry_block & ~exempt
    return force_exit, entry_block, exempt


def load_short_inputs(short_constraints: str) -> tuple[dict | None, dict | None]:
    """(calendar, futures availability) for a short-constraint mode; None
    where the mode does not use it. Read once in the parent."""
    if short_constraints not in SHORT_CONSTRAINT_MODES:
        raise ValueError(f"short_constraints must be one of {SHORT_CONSTRAINT_MODES}, "
                         f"got {short_constraints!r}")
    if short_constraints == "off":
        return None, None
    from signal_backtest.short_constraints import load_calendar, load_futures_availability
    futures = load_futures_availability() if short_constraints == "margin_futures" else None
    return load_calendar(), futures


def short_args(calendar: dict | None, futures: dict | None, sid: str) -> tuple:
    """A stock's (short_events, futures_days) for a worker."""
    if calendar is None:
        return None, None
    return calendar.get(sid, []), None if futures is None else futures.get(sid, set())


def _process_one_stock(
    sid: str,
    factory_names: list[str],
    start_index: int,
    use_cache: bool = False,
    short_events: list | None = None,
    futures_days: set | None = None,
    adjusted: bool = False,
    total_return: bool = False,
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
        data = load_stock_data(sid, use_cache=use_cache, adjusted=adjusted)
    except Exception as e:
        for n in factory_names:
            skipped_by_sig[n].append((sid, f"load: {e}"))
        return sid, trades_by_sig, sides_by_sig, skipped_by_sig

    short_masks = apply_settings(sid, data, short_events, futures_days, total_return)

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
            force_exit, entry_block, cover_exempt = (short_masks if side == "short"
                                                     else (None, None, None))
            try:
                if tiers is not None:
                    result = run_side_backtest_tiered(
                        data, side, tiers,
                        exit_=exit_,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
                        force_exit=force_exit,
                        entry_block=entry_block,
                        cover_exempt=cover_exempt,
                    )
                else:
                    result = run_side_backtest(
                        data, side, entry, exit_, defense_rules,
                        start_index=start_index,
                        floor_period=floor_period,
                        initial_period=init_period,
                        flood_tight_init=flood_ti,
                        force_exit=force_exit,
                        entry_block=entry_block,
                        cover_exempt=cover_exempt,
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
    include_holdout: bool = False,
    short_constraints: str = "off",
    adjusted: bool = False,
    total_return: bool = False,
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

    ``short_constraints="margin"`` puts every short on margin: forced cover on
    the 融券最後回補日 and no entry while 停券 (signal_backtest.short_constraints);
    ``"margin_futures"`` does so only where stock futures were not usable.

    ``adjusted=True`` runs on back-adjusted prices (backtest.adjust);
    ``total_return=True`` keeps raw prices and puts the corporate actions held
    through into each trade's return.

    Returns ``{name: (trades_path, sides_path)}``.
    """
    if not factories:
        raise ValueError("factories must be non-empty")
    if short_constraints not in SHORT_CONSTRAINT_MODES:
        raise ValueError(f"short_constraints must be one of {SHORT_CONSTRAINT_MODES}, "
                         f"got {short_constraints!r}")
    if adjusted and total_return:
        raise ValueError("adjusted prices already give total returns; pass one of the two")
    calendar, futures = load_short_inputs(short_constraints)

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
        f"workers={workers}, cache={'on' if use_cache else 'off'}, "
        f"short_constraints={short_constraints}, adjusted={'on' if adjusted else 'off'}, "
        f"total_return={'on' if total_return else 'off'}",
        file=sys.stderr,
    )

    trades_by_sig: dict[str, list[Trade]] = {n: [] for n in factories}
    sides_by_sig: dict[str, list[SideResult]] = {n: [] for n in factories}
    skipped_by_sig: dict[str, list[tuple[str, str]]] = {n: [] for n in factories}

    factory_names = list(factories)
    t0 = time.time()

    if workers <= 1:
        results_iter = (
            _process_one_stock(sid, factory_names, start_index, use_cache, *short_args(calendar, futures, sid), adjusted,
                               total_return)
            for sid, _name in stocks
        )
    else:
        args_iter = [
            (sid, factory_names, start_index, use_cache, *short_args(calendar, futures, sid), adjusted, total_return)
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
        _write_trades(trades_by_sig[name], trades_path, include_holdout)
        _write_sides(sides_by_sig[name], sides_path, include_holdout)
        if skipped_by_sig[name]:
            (d / "skipped.txt").write_text(
                "\n".join(f"{sid}\t{reason}" for sid, reason in skipped_by_sig[name]),
                encoding="utf-8",
            )
        n_trades = len(_kept(trades_by_sig[name], include_holdout))
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
    include_holdout: bool = False,
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
        f"總交易 {len(_kept(all_trades, include_holdout)):,}，跳過 {len(skipped)}",
        file=sys.stderr,
    )

    trades_path = output_dir / "trades.parquet"
    sides_path = output_dir / "sides.parquet"

    _write_trades(all_trades, trades_path, include_holdout)
    _write_sides(all_sides, sides_path, include_holdout)

    if skipped:
        skipped_path = output_dir / "skipped.txt"
        skipped_path.write_text(
            "\n".join(f"{sid}\t{reason}" for sid, reason in skipped),
            encoding="utf-8",
        )

    return trades_path, sides_path


def _kept(trades: list[Trade], include_holdout: bool) -> list[Trade]:
    """The trades an output may show: none entered in the holdout
    (engine.HOLDOUT_START) unless the monthly reconciliation asks for them."""
    if include_holdout:
        return list(trades)
    return [t for t in trades if t.entry_date < HOLDOUT_START]


def _write_trades(trades: list[Trade], path: Path, include_holdout: bool = False) -> None:
    """One row per trade; defense_events serialized as a list of dicts."""
    rows = []
    for t in _kept(trades, include_holdout):
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


def _write_sides(sides: list[SideResult], path: Path, include_holdout: bool = False) -> None:
    """One row per (stock, side) — summary header without trade detail."""
    rows = []
    for s in sides:
        rows.append({
            "股票代號": s.stock_id,
            "股票名稱": s.stock_name,
            "方向": s.side,
            "起始日": s.start_date,
            "結束日": s.end_date,
            "交易數": len(_kept(s.trades, include_holdout)),
        })
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)
