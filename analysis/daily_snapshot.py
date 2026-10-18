"""Combined daily snapshot — three outputs from a single per-stock pass:

  1. Top-300 long/short ScoreBoard (tw.score_snapshot)
  2. Signal-factory fires for the 6 conditions (tw.signal_snapshot)
  3. Currently-open unified-strategy positions (tw.open_positions), and the
     previous night's positions this run no longer holds (tw.position_revisions)

Each stock is evaluated on every bar on record, as the backtest and the
intraday snapshot do. Stocks held the previous night are evaluated even when
their money_level has dropped below the cutoff, for their positions only.

Each worker loads a stock once, runs the 4-bar scoreboard evaluations,
the 6 condition functions on the latest bar, and the unified long/short
backtests, then returns a flat dict to the main process. Workers run in
parallel via multiprocessing.Pool.

Replaces the earlier separate score_snapshot / signal_snapshot /
position_snapshot modules; merging eliminates ~50% redundant
load_stock_data calls and a redundant active-stock query per snapshot.

Env vars:
  DAILY_SNAPSHOT_WORKERS      — worker count (default 8, tuned for 8 P-cores)
  DAILY_SNAPSHOT_MIN_LEVEL    — money_level cutoff (default 4)
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import date
from multiprocessing import Pool, cpu_count

from psycopg2.extras import execute_batch

from db.connection import get_cursor, get_connection
from db.price_availability import PriceUnavailable
from backtest.data import load_stock_data
from analysis.score import build_scoreboard
from analysis.snapshot_runs import build_commit, save_run
from analysis.snapshot_errors import PerStockErrors, describe, log_lines
from signal_backtest.factories._conditions import (
    pick_condition, touch_condition,
    buy_condition, sell_condition,
    buy_flee_signal, sell_flee_signal,
    post_break_blackout,
)
# Pull unified factories via SIGNAL_FACTORIES rather than direct import
# from signal_backtest.factories.unified — the latter has a circular
# import with signal_backtest.signal._register_factories.
from signal_backtest.signal import SIGNAL_FACTORIES
from signal_backtest.engine import run_side_backtest_tiered
from signal_backtest.trade import REASON_EXIT_END, REASON_ENTRY_INIT

unified_long_factory = SIGNAL_FACTORIES["unified_long"]
unified_short_factory = SIGNAL_FACTORIES["unified_short"]

TOP_N = 300
DEFAULT_WORKERS = max(1, min(cpu_count() or 1, 8))
N_WORKERS = int(os.environ.get("DAILY_SNAPSHOT_WORKERS", str(DEFAULT_WORKERS)))

# Skip stocks with money_level < this on the snapshot date. Default 4
# (8-day SMA turnover < 27M TWD; one tier above the 'dead fish' cutoff).
MIN_MONEY_LEVEL = int(os.environ.get("DAILY_SNAPSHOT_MIN_LEVEL", "4"))

# How far back a worker reports closed trades (bars), for telling an exit on a
# night without a snapshot from a position the rerun dropped. Snapshots have
# been missing two days at most (2026-04-29/30).
RECENT_EXIT_BARS = 20

# (signal_name, condition_fn) — kept in display order; CHECK constraint in
# tw.signal_snapshot enforces the same set of names.
SIGNALS = [
    ("pick",      pick_condition),
    ("touch",     touch_condition),
    ("buy",       buy_condition),
    ("sell",      sell_condition),
    ("buy_flee",  buy_flee_signal),
    ("sell_flee", sell_flee_signal),
]

# Defense event 0 has reason like '進場初始[pick]'; pull out 'pick'.
_TIER_RE = re.compile(rf"^{re.escape(REASON_ENTRY_INIT)}\[(\w+)\]$")


# ── Worker side ────────────────────────────────────────────────────────────

# ScoreBoard is heavy to construct (registers 30+ ScoreItem lambdas); each
# worker builds it once via Pool initializer and reuses across stocks.
_BOARD = None


def _init_worker() -> None:
    global _BOARD
    _BOARD = build_scoreboard()


def _eval_pct(data, idx: int, on_error=None) -> tuple[float | None, float | None]:
    if idx < 0:
        return (None, None)
    try:
        r = _BOARD.evaluate(data, idx)
    except Exception as e:
        if on_error is not None:
            on_error(e)
        return (None, None)
    return (r.total.long.pct, r.total.short.pct)


def _safe_num(x: float) -> float | None:
    """NaN -> None for DB; psycopg2 NUMERIC won't accept NaN."""
    return None if x != x else x


def _extract_open_position(result, side: str, last_trade_date) -> dict | None:
    """Inspect a SideResult; return position summary if the engine's last
    trade closed on the snapshot bar.

    Two cases by ``last.exit_reason``:
      1. exit_reason == REASON_EXIT_END → still open (force-closed at
         end-of-history); is_exited = False.
      2. exit_reason != REASON_EXIT_END → exited on the snapshot bar
         (real exit signal); is_exited = True, exit_reason carried over.

    Earlier bars are ignored — daily snapshot only surfaces today's state."""
    if not result.trades:
        return None
    last = result.trades[-1]
    if last.exit_date != last_trade_date:
        return None
    if not last.defense_events:
        return None

    m = _TIER_RE.match(last.defense_events[0].reason)
    if m is None:
        return None
    entry_tier = m.group(1)

    is_exited = last.exit_reason != REASON_EXIT_END
    cur_def = last.defense_events[-1]
    return {
        "side": side,
        "entry_date": last.entry_date,
        "entry_price": float(last.entry_price),
        "entry_tier": entry_tier,
        "current_close": float(last.exit_price),
        # Trade.pnl_pct is a fraction (0.107 = 10.7%); scale to percentage.
        "pnl_pct": float(last.pnl_pct) * 100.0,
        "bars_held": int(last.holding_days),
        "defense_price": _safe_num(float(cur_def.price)),
        "defense_reason": cur_def.reason,
        "defense_date": cur_def.date,
        "is_exited": is_exited,
        "exit_reason": last.exit_reason if is_exited else None,
    }


def _eval_stock(args: tuple[str, date, date]) -> tuple[dict | None, list]:
    """Worker-process entry. One load_stock_data per stock (ending at
    end_date so signals evaluate as-of snapshot_date even on backfill;
    start_date None reads every earlier bar); produces score pcts at 4 bars, signal-factory fires on the
    last bar within the window, and unified-strategy open-position info.
    Returns (flat dict so result pickles cheaply, or None when the stock is
    unusable, or {"_unavailable": sid} when its price is unavailable (emerging
    board); the exceptions it swallowed as (site, stock_id, message)).
    A stock with under 60 bars, or an unavailable price, is skipped, not an
    error."""
    sid, start_date, end_date = args
    errors: list[tuple[str, str, str]] = []

    def note(site):
        return lambda e: errors.append((site, sid, describe(e)))

    try:
        data = load_stock_data(sid, start_date=start_date, end_date=end_date)
    except PriceUnavailable:
        print(f"  [WARN] {sid} skipped: price unavailable (emerging board)", file=sys.stderr)
        return {"_unavailable": sid}, errors
    except Exception as e:
        note("load")(e)
        return None, errors
    if data.n < 60:
        return None, errors

    idx = data.n - 1
    last_date = data.dates[-1]

    # 1. ScoreBoard at 4 bars.
    lp, sp = _eval_pct(data, idx, note("score"))
    if lp is None:
        return None, errors
    lp_d1, sp_d1 = _eval_pct(data, idx - 1, note("score_d1"))
    lp_d2, sp_d2 = _eval_pct(data, idx - 2, note("score_d2"))
    lp_d3, sp_d3 = _eval_pct(data, idx - 3, note("score_d3"))

    # 2. Signal conditions on the latest bar.
    # The unified factories AND their entries with ~post_break_blackout; these
    # raw conditions bypass that wrapper, so apply the same seam filter here or
    # a stock resuming on a new price scale fires on windows that span the seam
    # (5904 fired buy_flee then sell_flee on 2026-08-10/11 at 0.11x its close).
    fires: list[str] = []
    if not bool(post_break_blackout(data)[idx]):
        for name, fn in SIGNALS:
            try:
                arr = fn(data)
            except Exception as e:
                note(f"signal {name}")(e)
                continue
            if bool(arr[idx]):
                fires.append(name)

    # 3. Unified-strategy open positions (full backtest each side).
    open_positions: list[dict] = []
    # Exits before the last bar, entry_date -> exit_date, back to RECENT_EXIT_BARS
    # bars: a position held at the previous snapshot that exited on a night
    # with no snapshot is told apart from one the rerun dropped.
    recent_exits: dict[str, dict] = {"long": {}, "short": {}}
    since = data.dates[max(0, data.n - 1 - RECENT_EXIT_BARS)]
    for side, factory in (
        ("long",  unified_long_factory),
        ("short", unified_short_factory),
    ):
        try:
            spec = factory(data)
        except Exception as e:
            note(f"unified {side}")(e)
            continue
        tiers = spec.long_tiers if side == "long" else spec.short_tiers
        exit_arr = spec.signals.long_exit if side == "long" else spec.signals.short_exit
        if not tiers:
            continue
        try:
            result = run_side_backtest_tiered(
                data, side, tiers,
                exit_=exit_arr,
                floor_period=spec.long_floor_period if side == "long"
                              else spec.short_floor_period,
            )
        except Exception as e:
            note(f"backtest {side}")(e)
            continue
        pos = _extract_open_position(result, side, last_date)
        if pos is not None:
            open_positions.append(pos)
        recent_exits[side] = {t.entry_date: t.exit_date for t in result.trades
                              if since <= t.exit_date < last_date}

    tv_raw = float(data.turnover[idx])
    tv = tv_raw if tv_raw == tv_raw else 0.0

    return {
        "sid": sid,
        "lp": lp, "sp": sp, "tv": tv,
        "lp_d1": lp_d1, "sp_d1": sp_d1,
        "lp_d2": lp_d2, "sp_d2": sp_d2,
        "lp_d3": lp_d3, "sp_d3": sp_d3,
        "fires": fires,
        "open_positions": open_positions,
        "recent_exits": recent_exits,
        # Before snapshot_date when the stock did not trade that day.
        "last_date": last_date,
        # Classified here rather than at export time: the pipeline already
        # ran, and re-deriving it downstream would cost a second pass.
        "vol_status": int(data.volume_result.volume_status[idx]),
    }, errors


# ── Main side ──────────────────────────────────────────────────────────────

# Per-market coverage floor vs the previous trading day. Below this ratio
# we assume an upstream scraper dropped that whole market and refuse to
# produce a snapshot — partial snapshots get shipped to Vercel and look
# correct at a glance (see 2026-06-03 incident: TWSE prices scraper failed,
# liquidity rows for TWSE went 0, snapshot ran on the surviving 973 / 2220
# stocks, positions.json was missing 群創 and the rest of TWSE).
LIQUIDITY_COVERAGE_MIN = 0.90


def _check_liquidity_fresh(snapshot_date: date) -> None:
    """Precondition: stock_liquidity_daily must have rows for snapshot_date,
    AND each of TWSE / TPEx must be >= LIQUIDITY_COVERAGE_MIN of the prior
    trading day's count. The bare ``COUNT > 0`` check is too weak — one
    market can go to zero while the other still satisfies it."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM tw.stock_liquidity_daily WHERE trade_date = %s",
            (snapshot_date,),
        )
        n_today = cur.fetchone()["n"]
        if n_today == 0:
            raise RuntimeError(
                f"tw.stock_liquidity_daily empty for {snapshot_date}. "
                f"Run analysis.daily_liquidity.compute_daily_liquidity({snapshot_date}) first."
            )

        cur.execute("""
            SELECT MAX(trade_date) AS d
            FROM tw.stock_liquidity_daily
            WHERE trade_date < %s
        """, (snapshot_date,))
        prev_date = cur.fetchone()["d"]
        if prev_date is None:
            return

        cur.execute("""
            SELECT s.market, l.trade_date, COUNT(*) AS n
            FROM tw.stock_liquidity_daily l
            JOIN tw.stocks s ON s.stock_id = l.stock_id
            WHERE l.trade_date IN (%s, %s)
              AND s.market IN ('TWSE', 'TPEx')
            GROUP BY s.market, l.trade_date
        """, (prev_date, snapshot_date))
        counts: dict[tuple[str, date], int] = {
            (r["market"], r["trade_date"]): r["n"] for r in cur.fetchall()
        }

    short: list[str] = []
    for market in ("TWSE", "TPEx"):
        prev_n = counts.get((market, prev_date), 0)
        today_n = counts.get((market, snapshot_date), 0)
        if prev_n == 0:
            continue
        if today_n < prev_n * LIQUIDITY_COVERAGE_MIN:
            ratio = today_n / prev_n if prev_n else 0.0
            short.append(f"{market} {today_n}/{prev_n} ({ratio:.0%})")
    if short:
        raise RuntimeError(
            f"tw.stock_liquidity_daily coverage shortfall on {snapshot_date} "
            f"vs {prev_date}: {', '.join(short)} (min {LIQUIDITY_COVERAGE_MIN:.0%}). "
            f"Likely an upstream price scraper dropped this market. "
            f"Re-run scrapers.twse / scrapers.tpex for {snapshot_date}, then "
            f"compute_daily_liquidity({snapshot_date})."
        )


def _load_active_stock_ids(snapshot_date: date) -> list[str]:
    """Active TWSE/TPEx stocks with money_level >= MIN_MONEY_LEVEL on the
    snapshot date. Dead-fish (level < 3) are excluded — their signals are
    too noisy from anaemic turnover."""
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT s.stock_id
            FROM tw.stocks s
            JOIN tw.stock_liquidity_daily l
              ON l.stock_id = s.stock_id AND l.trade_date = %s
            WHERE s.is_active = TRUE
              AND s.security_type = 'STOCK'
              AND s.market IN ('TWSE', 'TPEx')
              AND l.money_level >= %s
            ORDER BY s.stock_id
        """, (snapshot_date, MIN_MONEY_LEVEL))
        return [r["stock_id"] for r in cur.fetchall()]


# The latest nightly snapshot before a date. Its open (not exited) positions
# are what that date's run is expected to carry on or exit.
_PREV_HELD = """
    FROM tw.open_positions p
    WHERE p.snapshot_date = (SELECT MAX(snapshot_date) FROM tw.open_positions
                             WHERE snapshot_date < %s)
      AND NOT p.is_exited
"""


def load_held_stock_ids(snapshot_date: date) -> list[str]:
    """Stocks with an open position in the latest nightly snapshot before
    snapshot_date, among the active TWSE/TPEx stocks. Evaluated whatever
    their money_level, so a position whose stock turned illiquid is still
    followed to its exit instead of dropping out of the table unrecorded.
    The intraday snapshot uses the same set."""
    with get_cursor(commit=False) as cur:
        cur.execute(f"""
            SELECT DISTINCT p.stock_id
            {_PREV_HELD}
              AND p.stock_id IN (SELECT stock_id FROM tw.stocks
                                 WHERE is_active = TRUE AND security_type = 'STOCK'
                                   AND market IN ('TWSE', 'TPEx'))
            ORDER BY p.stock_id
        """, (snapshot_date,))
        return [r["stock_id"] for r in cur.fetchall()]


def _classify_revisions(prev_rows: list[dict], results: list[dict]) -> tuple[list[dict], dict]:
    """Each position held in the previous nightly snapshot ends this run as
    exactly one of:
      kept           the same (stock, side, entry_date) is still held
      exited         the same key carries an exit row today
      exited_unseen  it exited after the previous snapshot and before today,
                     on a night with no snapshot (a real exit no row showed)
      redated        the stock is held, or exited today, on that side from
                     another entry date
      gone           the stock was evaluated and has nothing on that side
      not_run        the stock was not evaluated (inactive, price unavailable,
                     too short a history)
    Returns the rows of the last four, which tw.position_revisions keeps,
    and the count of each."""
    evaluated = {r["sid"]: r for r in results}
    today = {(r["sid"], p["side"]): p for r in results for p in r["open_positions"]}
    counts = {k: 0 for k in ("kept", "exited", "exited_unseen", "redated", "gone", "not_run")}
    revisions = []
    for row in prev_rows:
        now = today.get((row["stock_id"], row["side"]))
        if now is not None and now["entry_date"] == row["entry_date"]:
            counts["exited" if now["is_exited"] else "kept"] += 1
            continue
        extra = {"new_entry_date": None, "new_is_exited": None, "exit_date": None}
        r = evaluated.get(row["stock_id"])
        exit_date = r and r["recent_exits"][row["side"]].get(row["entry_date"])
        if r is None:
            cause = "not_run"
        elif exit_date is not None and exit_date > row["snapshot_date"]:
            cause = "exited_unseen"
            extra["exit_date"] = exit_date
        elif now is not None:
            cause = "redated"
            extra["new_entry_date"] = now["entry_date"]
            extra["new_is_exited"] = now["is_exited"]
        else:
            cause = "gone"
        counts[cause] += 1
        revisions.append({**row, "cause": cause, **extra})
    assert sum(counts.values()) == len(prev_rows), (counts, len(prev_rows))
    return revisions, counts


def _load_prev_score_ranks(snapshot_date: date, side: str) -> dict[str, int]:
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT MAX(snapshot_date) AS d
            FROM tw.score_snapshot
            WHERE snapshot_date < %s AND side = %s
        """, (snapshot_date, side))
        prev_date = cur.fetchone()["d"]
        if prev_date is None:
            return {}
        cur.execute("""
            SELECT stock_id, rank FROM tw.score_snapshot
            WHERE snapshot_date = %s AND side = %s
        """, (prev_date, side))
        return {r["stock_id"]: r["rank"] for r in cur.fetchall()}


def _save_score_side(snapshot_date: date, side: str, results: list[dict],
                     prev: dict[str, int]) -> int:
    """Sort by side's pct desc (turnover tie-breaker), keep top N, write."""
    if side == "long":
        ranked = sorted(results, key=lambda r: (-r["lp"], -r["tv"]))[:TOP_N]
        pct_key, d1_key, d2_key, d3_key = "lp", "lp_d1", "lp_d2", "lp_d3"
    else:
        ranked = sorted(results, key=lambda r: (-r["sp"], -r["tv"]))[:TOP_N]
        pct_key, d1_key, d2_key, d3_key = "sp", "sp_d1", "sp_d2", "sp_d3"

    rows = []
    for i, r in enumerate(ranked, start=1):
        sid = r["sid"]
        prev_rank = prev.get(sid)
        is_new = prev_rank is None
        rank_delta = (prev_rank - i) if prev_rank is not None else None
        rows.append((
            snapshot_date, side, i, sid,
            round(r[pct_key], 3), round(r["tv"], 2),
            is_new, prev_rank, rank_delta,
            round(r[d1_key], 3) if r[d1_key] is not None else None,
            round(r[d2_key], 3) if r[d2_key] is not None else None,
            round(r[d3_key], 3) if r[d3_key] is not None else None,
        ))

    sql = """
        INSERT INTO tw.score_snapshot
        (snapshot_date, side, rank, stock_id, total_pct, turnover,
         is_new, prev_rank, rank_delta, pct_d1, pct_d2, pct_d3)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.score_snapshot WHERE snapshot_date = %s AND side = %s",
            (snapshot_date, side),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _save_signal_fires(snapshot_date: date, results: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {name: 0 for name, _ in SIGNALS}
    rows = []
    for r in results:
        for name in r["fires"]:
            counts[name] += 1
            rows.append((snapshot_date, name, r["sid"], round(r["tv"], 2)))

    sql = """
        INSERT INTO tw.signal_snapshot (snapshot_date, signal, stock_id, turnover)
        VALUES (%s, %s, %s, %s)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM tw.signal_snapshot WHERE snapshot_date = %s",
            (snapshot_date,),
        )
        execute_batch(cur, sql, rows)
        conn.commit()
    finally:
        conn.close()
    return counts


def _save_open_positions(snapshot_date: date, results: list[dict]) -> dict[str, int]:
    counts = {"long": 0, "short": 0, "exited_long": 0, "exited_short": 0}
    rows = []
    for r in results:
        for p in r["open_positions"]:
            bucket = ("exited_" if p["is_exited"] else "") + p["side"]
            counts[bucket] += 1
            rows.append((
                snapshot_date,
                r["sid"],
                p["side"],
                p["entry_date"],
                round(p["entry_price"], 2),
                p["entry_tier"],
                round(p["current_close"], 2),
                round(p["pnl_pct"], 3),
                p["bars_held"],
                round(r["tv"], 2),
                round(p["defense_price"], 2) if p["defense_price"] is not None else None,
                p["defense_reason"],
                p["defense_date"],
                p["is_exited"],
                p["exit_reason"],
                r.get("vol_status"),
            ))

    sql = """
        INSERT INTO tw.open_positions
        (snapshot_date, stock_id, side, entry_date, entry_price, entry_tier,
         current_close, pnl_pct, bars_held, turnover,
         defense_price, defense_reason, defense_date, is_exited, exit_reason,
         volume_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # The previous night's positions this run drops are written with the
    # positions, in one transaction: a rerun of the date replaces both.
    commit = build_commit()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT p.snapshot_date, p.stock_id, p.side, p.entry_date, "
                    f"p.entry_price, p.entry_tier {_PREV_HELD}", (snapshot_date,))
        cols = [c[0] for c in cur.description]
        prev_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        revisions, revision_counts = _classify_revisions(prev_rows, results)
        cur.execute(
            "DELETE FROM tw.open_positions WHERE snapshot_date = %s",
            (snapshot_date,),
        )
        execute_batch(cur, sql, rows)
        cur.execute("DELETE FROM tw.position_revisions WHERE snapshot_date = %s",
                    (snapshot_date,))
        if revisions:
            execute_batch(cur, """
                INSERT INTO tw.position_revisions
                (snapshot_date, prev_snapshot_date, stock_id, side, entry_date,
                 entry_price, entry_tier, cause, new_entry_date, new_is_exited,
                 exit_date, build_commit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [(snapshot_date, v["snapshot_date"], v["stock_id"], v["side"],
                   v["entry_date"], v["entry_price"], v["entry_tier"], v["cause"],
                   v["new_entry_date"], v["new_is_exited"], v["exit_date"], commit)
                  for v in revisions])
        conn.commit()
    finally:
        conn.close()
    # Revisions proper, and the exits only a night without a snapshot hid.
    counts["revisions"] = len(revisions) - revision_counts["exited_unseen"]
    counts["exited_unseen"] = revision_counts["exited_unseen"]
    counts["revision_counts"] = revision_counts
    return counts


def _check_market_breadth_fresh(snapshot_date: date) -> None:
    """Precondition: tw.market_breadth must reach snapshot_date.

    load_stock_data() pulls market_state from this table; stale rows
    silently degrade signal evaluation rather than failing loudly. We
    enforce the freshness contract here so manual / cron callers fail
    fast instead of producing snapshots with mixed-vintage market state.
    """
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT MAX(trade_date) AS d FROM tw.market_breadth")
        latest = cur.fetchone()["d"]
    if latest is None or latest < snapshot_date:
        raise RuntimeError(
            f"tw.market_breadth latest = {latest}, snapshot_date = {snapshot_date}. "
            f"Run analysis.market_breadth.calculate_market_breadth + save first."
        )


def _check_market_vol_fresh(snapshot_date: date) -> None:
    """Precondition: tw.market_vol must reach snapshot_date.

    The ScoreBoard 波動 cell divides each stock's Parkinson-233 volatility by this
    day's cross-sectional median. A missing row makes the cell score 0 for every
    stock — silently dropping a ±37.5-point cell rather than failing — which would
    shift every long_pct and quietly move the signal-factory gates.
    """
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT MAX(trade_date) AS d FROM tw.market_vol")
        latest = cur.fetchone()["d"]
    if latest is None or latest < snapshot_date:
        raise RuntimeError(
            f"tw.market_vol latest = {latest}, snapshot_date = {snapshot_date}. "
            f"Run python -m analysis.build_market_vol first."
        )


def run(snapshot_date: date) -> dict:
    """Main entry. Parallelizes per-stock evaluation across N_WORKERS,
    then writes both score and signal snapshots."""
    _check_market_breadth_fresh(snapshot_date)
    _check_market_vol_fresh(snapshot_date)
    _check_liquidity_fresh(snapshot_date)

    t0 = time.time()
    print(f"  Daily snapshot @ {snapshot_date} (workers={N_WORKERS}, min_level={MIN_MONEY_LEVEL}, all history) ...")
    active_ids = _load_active_stock_ids(snapshot_date)
    print(f"  Loaded {len(active_ids)} active stocks (after dead-fish filter)", file=sys.stderr)
    # Held last night but below the cutoff today: positions only, kept out of
    # the score ranking and the signal cards.
    active = set(active_ids)
    held_only = sorted(set(load_held_stock_ids(snapshot_date)) - active)
    if held_only:
        print(f"  {len(held_only)} held stocks below the cutoff, positions only: "
              f"{', '.join(held_only)}")
    stock_ids = active_ids + held_only

    # No start date: every bar on record, as the backtest and the intraday
    # snapshot see it. A rolling window changed which positions were open.
    work_items = [(sid, None, snapshot_date) for sid in stock_ids]

    results: list[dict] = []
    errors: list[tuple[str, str, str]] = []
    # Codes skipped for an unavailable price. Skipping is by design (emerging
    # board), but the universe only holds TWSE/TPEx codes, so any name here
    # is a listed stock that picked up an unpriced row -- keep them visible.
    unavailable: list[str] = []
    with Pool(N_WORKERS, initializer=_init_worker) as pool:
        for r, errs in pool.imap_unordered(_eval_stock, work_items, chunksize=20):
            errors.extend(errs)
            if r is not None and "_unavailable" in r:
                unavailable.append(r["_unavailable"])
            elif r is not None:
                results.append(r)
                if len(results) % 200 == 0:
                    print(f"  evaluated {len(results)} ...", file=sys.stderr)

    print(f"  scored {len(results)} stocks in {time.time() - t0:.1f}s")
    if unavailable:
        print(f"  [WARN] {len(unavailable)} stocks skipped: price unavailable (emerging board): "
              f"{', '.join(sorted(unavailable))}")
    # Nothing is written when any stock's evaluation raised: a step broken
    # for every stock would otherwise publish "no signal, no position".
    if errors:
        for line in log_lines(errors):
            print(line)
        raise PerStockErrors(errors)

    # The run record goes first: if it cannot be written nothing is, and a
    # rerun that fails later cannot leave the date's record naming an older build.
    ranked = [r for r in results if r["sid"] in active]
    save_run("daily", snapshot_date, None, None, len(stock_ids), len(ranked))

    prev_long  = _load_prev_score_ranks(snapshot_date, "long")
    prev_short = _load_prev_score_ranks(snapshot_date, "short")
    long_n  = _save_score_side(snapshot_date, "long",  ranked, prev_long)
    short_n = _save_score_side(snapshot_date, "short", ranked, prev_short)
    print(f"  Score top-{TOP_N} long: {long_n} / short: {short_n}")

    counts = _save_signal_fires(snapshot_date, ranked)
    for name, _ in SIGNALS:
        print(f"  signal {name}: {counts[name]}")

    pos_counts = _save_open_positions(snapshot_date, results)
    print(f"  Open positions long: {pos_counts['long']} / short: {pos_counts['short']}")
    print(f"  Exited today    long: {pos_counts['exited_long']} / short: {pos_counts['exited_short']}")
    rc = pos_counts["revision_counts"]
    print(f"  Previous night's positions: kept {rc['kept']}, exited {rc['exited']}, "
          f"exited unseen {rc['exited_unseen']}, redated {rc['redated']}, "
          f"gone {rc['gone']}, not run {rc['not_run']}")
    stale = sorted(r["sid"] for r in results if r["last_date"] < snapshot_date)
    if stale:
        print(f"  {len(stale)} stocks without a bar on {snapshot_date}, positions as of "
              f"their last bar: {', '.join(stale)}")

    # 大台期貨統一狀態 (EOD, pure DB history). Non-fatal.
    from analysis.tx_status import compute_and_save_tx_status
    tx = compute_and_save_tx_status(intraday=False)
    if tx is not None:
        print(f"  TX futures unified state: {tx['state']} @ {tx['current_close']:.0f}")

    print(f"  Total wall time: {time.time() - t0:.1f}s")
    return {
        "stocks_evaluated": len(results),
        "stocks_unavailable": sorted(unavailable),
        "score_long": long_n,
        "score_short": short_n,
        "signal_counts": counts,
        "position_counts": pos_counts,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today()
    run(d)
