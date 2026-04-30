"""ORB strategy backtest over tw.intraday_orb_signals + tw.intraday_opening_range.

For every ORB signal row, evaluates 3 exit strategies × 2 entry models and
writes the resulting 6 trade variants to tw.orb_backtest_trades.

Exit strategies:
  PT1_exit         Target = PT1. Stop = reversed (opposite OR side).
  PT2_exit         Target = PT2. Stop = reversed.
  PT1_then_trail   Initial stop = reversed. When PT1 hits, stop moves to
                   breakout_price. If reversed fires AFTER pt1_hit_at, we
                   infer the trail stop was hit somewhere in between (since
                   crossing the opposite OR from beyond pt1 necessarily
                   crosses breakout_price first) — exit at breakout_price
                   with exit_at ≈ midpoint(pt1_hit_at, reversed_at).

Entry methods:
  level            Enter at or_high (U) or or_low (D) — idealized breakout
  bar_close        Enter at the breakout 1-min bar's close — always ≥ level
                   for U, ≤ level for D. More realistic slippage model.

Costs: flat 0.6% round-trip (commission 0.1425%×2 + tax 0.3%, rounded).
Applied equally to long and short legs.

CLI:
  python -m backtest.orb_strategy           # full backtest over all signals
  python -m backtest.orb_strategy 2026-03-01 2026-04-22   # restricted range
"""

from __future__ import annotations

import csv
import sys
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from db.connection import get_cursor, init_db


_TPE_TZ        = timezone(timedelta(hours=8))
_SESSION_CLOSE = dtime(hour=13, minute=30)

_PT1_MULT         = 0.5
_PT2_MULT         = 1.0
_ROUND_TRIP_COST  = 0.006        # 0.6%

STRATEGIES = ("PT1_exit", "PT2_exit", "PT1_then_trail")
ENTRY_METHODS = ("level", "bar_close")


# ---------------------------------------------------------------------------
# Per-signal trade evaluation
# ---------------------------------------------------------------------------

def _compute_trade(sig: dict, strategy: str, entry_method: str) -> dict | None:
    """Return a trade dict for one (signal, strategy, entry_method) triple.

    Returns None if the signal can't produce a valid trade for this variant
    (e.g., entry_method='bar_close' on an old signal row with no bar close).
    """
    direction = sig["direction"]
    or_high = float(sig["or_high"])
    or_low = float(sig["or_low"])
    or_range = float(sig["or_range"])
    breakout_at = sig["breakout_at"]

    if entry_method == "level":
        entry_price = or_high if direction == "U" else or_low
    else:  # bar_close
        if sig["breakout_bar_close"] is None:
            return None
        entry_price = float(sig["breakout_bar_close"])

    if direction == "U":
        pt1_level = or_high + _PT1_MULT * or_range
        pt2_level = or_high + _PT2_MULT * or_range
        reversed_level = or_low
        breakout_price = or_high
    else:
        pt1_level = or_low - _PT1_MULT * or_range
        pt2_level = or_low - _PT2_MULT * or_range
        reversed_level = or_high
        breakout_price = or_low

    pt1_at = sig["pt1_hit_at"]
    pt2_at = sig["pt2_hit_at"]
    reversed_at = sig["reversed_at"]
    eod_close_raw = sig["eod_close"]
    eod_close = float(eod_close_raw) if eod_close_raw is not None else None
    eod_time = datetime.combine(sig["trade_date"], _SESSION_CLOSE, tzinfo=_TPE_TZ)

    exit_reason, exit_price, exit_at = _evaluate_exit(
        strategy, pt1_at, pt2_at, reversed_at,
        pt1_level, pt2_level, reversed_level, breakout_price,
        eod_close, eod_time,
    )

    if exit_price is None:
        # EOD close missing (halted / delisted) — no trade.
        return None

    if direction == "U":
        pnl_gross = (exit_price - entry_price) / entry_price
    else:
        pnl_gross = (entry_price - exit_price) / entry_price
    pnl_net = pnl_gross - _ROUND_TRIP_COST

    duration_min = None
    if breakout_at and exit_at:
        duration_min = int((exit_at - breakout_at).total_seconds() / 60)

    return {
        "strategy":      strategy,
        "entry_method":  entry_method,
        "direction":     direction,
        "entry_price":   entry_price,
        "entry_at":      breakout_at,
        "exit_price":    exit_price,
        "exit_at":       exit_at,
        "exit_reason":   exit_reason,
        "pnl_pct_gross": pnl_gross,
        "pnl_pct_net":   pnl_net,
        "duration_min":  duration_min,
    }


def _evaluate_exit(strategy, pt1_at, pt2_at, reversed_at,
                   pt1_level, pt2_level, reversed_level, breakout_price,
                   eod_close, eod_time):
    """Return (exit_reason, exit_price, exit_at).

    exit_price may be None if eod_close is None AND no target/stop fired.
    """
    if strategy == "PT1_exit":
        candidates = []
        if pt1_at:      candidates.append(("pt1", pt1_level, pt1_at))
        if reversed_at: candidates.append(("reversed", reversed_level, reversed_at))
        if candidates:
            candidates.sort(key=lambda x: x[2])
            return candidates[0]
        return ("eod_close", eod_close, eod_time)

    if strategy == "PT2_exit":
        candidates = []
        if pt2_at:      candidates.append(("pt2", pt2_level, pt2_at))
        if reversed_at: candidates.append(("reversed", reversed_level, reversed_at))
        if candidates:
            candidates.sort(key=lambda x: x[2])
            return candidates[0]
        return ("eod_close", eod_close, eod_time)

    if strategy == "PT1_then_trail":
        # Phase 1: before pt1 hit (or if pt1 never hit) — stop = reversed
        if reversed_at and (not pt1_at or reversed_at < pt1_at):
            return ("reversed", reversed_level, reversed_at)

        if pt1_at:
            # Phase 2: trail stop at breakout_price is now active
            pt2_before_rev = (
                pt2_at is not None
                and (reversed_at is None or pt2_at < reversed_at)
            )
            if pt2_before_rev:
                return ("pt2", pt2_level, pt2_at)
            # If price later crossed the opposite OR, it had to cross
            # breakout_price first on the way → trail stop triggered.
            if reversed_at and reversed_at > pt1_at:
                mid = pt1_at + (reversed_at - pt1_at) / 2
                return ("trail_stop", breakout_price, mid)
            # Held past PT1 to close without further triggers.
            return ("eod_close", eod_close, eod_time)

        # pt1 never hit, no reversal → price lingered, EOD close.
        return ("eod_close", eod_close, eod_time)

    raise ValueError(f"unknown strategy: {strategy}")


# ---------------------------------------------------------------------------
# DB I/O
# ---------------------------------------------------------------------------

def _fetch_signals(trade_date_range: tuple[date, date] | None) -> list[dict]:
    with get_cursor(commit=False) as cur:
        where = ""
        params: list = []
        if trade_date_range:
            start, end = trade_date_range
            where = "WHERE s.trade_date BETWEEN %s AND %s"
            params = [start, end]
        cur.execute(
            f"""
            SELECT
                s.trade_date, s.stock_id, s.direction,
                s.breakout_at, s.pt1_hit_at, s.pt2_hit_at, s.reversed_at,
                s.breakout_bar_close,
                r.or_high, r.or_low, r.or_range,
                dp.close_price AS eod_close
            FROM tw.intraday_orb_signals s
            JOIN tw.intraday_opening_range r
                ON r.trade_date = s.trade_date AND r.stock_id = s.stock_id
            LEFT JOIN tw.daily_prices dp
                ON dp.trade_date = s.trade_date AND dp.stock_id = s.stock_id
            {where}
            ORDER BY s.trade_date, s.stock_id
            """,
            params,
        )
        return cur.fetchall()


def _upsert_trades(trades: list[dict]) -> int:
    if not trades:
        return 0
    rows = [
        (
            t["trade_date"], t["stock_id"], t["strategy"], t["entry_method"],
            t["direction"], t["entry_price"], t["entry_at"],
            t["exit_price"], t["exit_at"], t["exit_reason"],
            t["pnl_pct_gross"], t["pnl_pct_net"], t["duration_min"],
        )
        for t in trades
    ]
    with get_cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO tw.orb_backtest_trades
                (trade_date, stock_id, strategy, entry_method, direction,
                 entry_price, entry_at, exit_price, exit_at, exit_reason,
                 pnl_pct_gross, pnl_pct_net, duration_min)
            VALUES %s
            ON CONFLICT (trade_date, stock_id, strategy, entry_method) DO UPDATE SET
                direction      = EXCLUDED.direction,
                entry_price    = EXCLUDED.entry_price,
                entry_at       = EXCLUDED.entry_at,
                exit_price     = EXCLUDED.exit_price,
                exit_at        = EXCLUDED.exit_at,
                exit_reason    = EXCLUDED.exit_reason,
                pnl_pct_gross  = EXCLUDED.pnl_pct_gross,
                pnl_pct_net    = EXCLUDED.pnl_pct_net,
                duration_min   = EXCLUDED.duration_min
            """,
            rows,
        )
    return len(rows)


# ---------------------------------------------------------------------------
# Summary + CSV
# ---------------------------------------------------------------------------

def print_summary(trade_date_range: tuple[date, date] | None = None) -> None:
    with get_cursor(commit=False) as cur:
        where = ""
        params: list = []
        if trade_date_range:
            start, end = trade_date_range
            where = "WHERE trade_date BETWEEN %s AND %s"
            params = [start, end]
        cur.execute(
            f"""
            SELECT
                strategy, entry_method, direction,
                COUNT(*)                                            AS n,
                SUM(CASE WHEN pnl_pct_net > 0 THEN 1 ELSE 0 END)    AS wins,
                AVG(pnl_pct_net)                                    AS avg_net,
                AVG(CASE WHEN pnl_pct_net > 0  THEN pnl_pct_net END) AS avg_win,
                AVG(CASE WHEN pnl_pct_net <= 0 THEN pnl_pct_net END) AS avg_loss,
                SUM(pnl_pct_net)                                    AS sum_net,
                AVG(duration_min)                                   AS avg_dur
            FROM tw.orb_backtest_trades
            {where}
            GROUP BY strategy, entry_method, direction
            ORDER BY strategy, entry_method, direction
            """,
            params,
        )
        rows = cur.fetchall()

    hdr = (f"{'strategy':<16} {'entry':<10} {'dir':<4} {'n':>6}  "
           f"{'win%':>6}  {'avg_net':>8}  {'avg_win':>8}  {'avg_loss':>9}  "
           f"{'PF':>5}  {'dur_min':>7}  {'tot_net':>8}")
    print()
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        n = int(r["n"])
        wins = int(r["wins"])
        losses = n - wins
        avg_win = float(r["avg_win"] or 0)
        avg_loss = float(r["avg_loss"] or 0)
        win_pct = 100.0 * wins / n if n else 0
        if losses and avg_loss < 0:
            pf = (avg_win * wins) / (abs(avg_loss) * losses)
            pf_str = f"{pf:>5.2f}"
        else:
            pf_str = "  inf"
        print(
            f"{r['strategy']:<16} {r['entry_method']:<10} {r['direction']:<4} "
            f"{n:>6}  {win_pct:>5.1f}%  "
            f"{float(r['avg_net'])*100:>7.3f}%  "
            f"{avg_win*100:>7.3f}%  {avg_loss*100:>8.3f}%  "
            f"{pf_str}  {float(r['avg_dur'] or 0):>7.1f}  "
            f"{float(r['sum_net'])*100:>7.1f}%"
        )


def export_csv(path: Path, trade_date_range: tuple[date, date] | None = None) -> int:
    with get_cursor(commit=False) as cur:
        where = ""
        params: list = []
        if trade_date_range:
            start, end = trade_date_range
            where = "WHERE trade_date BETWEEN %s AND %s"
            params = [start, end]
        cur.execute(
            f"""
            SELECT trade_date, stock_id, strategy, entry_method, direction,
                   entry_price, entry_at, exit_price, exit_at, exit_reason,
                   pnl_pct_gross, pnl_pct_net, duration_min
            FROM tw.orb_backtest_trades
            {where}
            ORDER BY trade_date, stock_id, strategy, entry_method
            """,
            params,
        )
        rows = cur.fetchall()

    if not rows:
        print(f"No trades to export to {path}.")
        return 0

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Exported {len(rows)} trades to {path}")
    return len(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_backtest(trade_date_range: tuple[date, date] | None = None) -> int:
    signals = _fetch_signals(trade_date_range)
    print(f"Evaluating {len(signals)} ORB signals × 6 variants ...")

    batch: list[dict] = []
    total = 0
    for sig in signals:
        for strategy in STRATEGIES:
            for entry_method in ENTRY_METHODS:
                trade = _compute_trade(sig, strategy, entry_method)
                if trade is None:
                    continue
                trade["trade_date"] = sig["trade_date"]
                trade["stock_id"] = sig["stock_id"]
                batch.append(trade)
        if len(batch) >= 5000:
            total += _upsert_trades(batch)
            batch.clear()
    if batch:
        total += _upsert_trades(batch)

    print(f"Upserted {total} trade rows.")
    return total


def _parse_args(argv: list[str]) -> tuple[date, date] | None:
    if not argv:
        return None
    if len(argv) != 2:
        raise SystemExit("Usage: python -m backtest.orb_strategy [start_date end_date]")
    return date.fromisoformat(argv[0]), date.fromisoformat(argv[1])


if __name__ == "__main__":
    init_db()
    rng = _parse_args(sys.argv[1:])
    run_backtest(rng)
    print_summary(rng)
    export_csv(Path("orb_backtest_trades.csv"), rng)
