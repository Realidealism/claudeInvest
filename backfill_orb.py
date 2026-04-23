"""One-off backfill for tw.intraday_opening_range + tw.intraday_orb_signals.

Pulls historical 1-minute K-bars from SinoPac Shioaji for every stock in
the current ORB watchlist, then reconstructs both the opening range (OR)
and the full sequence of breakout / PT1 / PT2 / reversal events exactly as
the live intraday.orb.run() thread would have.

Usage:
  python backfill_orb.py                 # last 60 trading days + today
  python backfill_orb.py 30              # last 30 trading days + today
  python backfill_orb.py 2026-02-01 2026-04-22  # explicit inclusive range

Requires SINOPAC_API_KEY / SINOPAC_SECRET_KEY in .env. No order placing.
"""

from __future__ import annotations

import sys
import time
import traceback
from datetime import date, datetime, time as dtime, timedelta, timezone

from db.connection import get_cursor, init_db
from intraday.orb import build_orb_watchlist
from intraday.sinopac_loader import load_api, logout_api


_TPE_TZ         = timezone(timedelta(hours=8))
_SESSION_OPEN   = dtime(hour=9,  minute=0)
_OR_END         = dtime(hour=9,  minute=30)
_SESSION_CLOSE  = dtime(hour=13, minute=30)
_PT1_MULT       = 0.5
_PT2_MULT       = 1.0

_DEFAULT_LOOKBACK = 60
_INTER_CALL_SLEEP = 0.2   # seconds — stay well under Shioaji burst limit


# ---------------------------------------------------------------------------
# Date selection
# ---------------------------------------------------------------------------

def _recent_trading_days(n: int) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX'
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (n,),
        )
        return sorted(r["trade_date"] for r in cur.fetchall())


def _trading_days_in_range(start: date, end: date) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start, end),
        )
        return [r["trade_date"] for r in cur.fetchall()]


def _maybe_add_today(dates: list[date]) -> list[date]:
    """Include today if it's a weekday and not already in the TAIEX list.

    TAIEX is usually scraped post-close, so intraday rebuild runs during/after
    the day won't see today's TAIEX row yet.
    """
    today = date.today()
    if today.weekday() >= 5:
        return dates
    if today in dates:
        return dates
    return sorted(dates + [today])


# ---------------------------------------------------------------------------
# Shioaji contract lookup
# ---------------------------------------------------------------------------

def _find_contract(api, code: str):
    """Return the Shioaji Stock contract for code, or None."""
    for ns_name in ("TSE", "OTC"):
        ns = getattr(api.Contracts.Stocks, ns_name, None)
        if ns is None:
            continue
        try:
            contract = ns[code]
            if contract is not None:
                return contract
        except (KeyError, TypeError, AttributeError):
            continue
    return None


# ---------------------------------------------------------------------------
# K-bar parsing
# ---------------------------------------------------------------------------

def _group_bars_by_date(kbars) -> dict[date, list[dict]]:
    """Split Shioaji kbars into per-date chronological lists.

    Shioaji's KBars carries parallel arrays (ts, Open, High, Low, Close, Volume).
    ts stores TPE wall-clock time as UTC-labeled epoch nanoseconds (a known
    SinoPac quirk), so we interpret it as UTC and re-label without any offset
    to recover the TPE local time correctly.
    """
    out: dict[date, list[dict]] = {}
    ts_arr = getattr(kbars, "ts", None)
    if ts_arr is None or len(ts_arr) == 0:
        return out

    opens = kbars.Open
    highs = kbars.High
    lows = kbars.Low
    closes = kbars.Close
    vols = kbars.Volume

    for i in range(len(ts_arr)):
        dt = datetime.fromtimestamp(int(ts_arr[i]) / 1e9, tz=timezone.utc).replace(tzinfo=_TPE_TZ)
        out.setdefault(dt.date(), []).append({
            "time":  dt,
            "open":  float(opens[i]),
            "high":  float(highs[i]),
            "low":   float(lows[i]),
            "close": float(closes[i]),
            "volume": int(vols[i]),
        })

    for bars in out.values():
        bars.sort(key=lambda b: b["time"])
    return out


# ---------------------------------------------------------------------------
# OR + signal reconstruction
# ---------------------------------------------------------------------------

def _compute_opening_range(bars: list[dict]) -> tuple[float, float] | None:
    """Return (or_high, or_low) from 09:00 <= bar_time <= 09:30 inclusive.

    Returns None if the window has no bars or collapses to a single price
    (common for halted / extremely illiquid names in that window).
    """
    in_window = [b for b in bars if _SESSION_OPEN <= b["time"].time() <= _OR_END]
    if not in_window:
        return None
    or_high = max(b["high"] for b in in_window)
    or_low = min(b["low"] for b in in_window)
    if or_high <= or_low:
        return None
    return or_high, or_low


def _replay_signals(bars: list[dict], or_high: float, or_low: float) -> dict | None:
    """Walk 09:30 < t <= 13:30 bars and return the first occurrence of each stage.

    For upside-direction signals we use bar.high for PT crossings (the bar
    actually touched that price) and bar.low for reversal. Mirror for downside.
    If a single bar crosses both OR boundaries we use bar.open to resolve
    which direction was triggered first.
    """
    post = [b for b in bars if _OR_END < b["time"].time() <= _SESSION_CLOSE]
    if not post:
        return None

    or_range = or_high - or_low
    pt1_up = or_high + _PT1_MULT * or_range
    pt2_up = or_high + _PT2_MULT * or_range
    pt1_dn = or_low - _PT1_MULT * or_range
    pt2_dn = or_low - _PT2_MULT * or_range

    direction = None
    breakout_price = None
    breakout_at = None
    breakout_bar = None
    pt1_at = None
    pt2_at = None
    reversed_at = None

    for bar in post:
        if direction is None:
            hit_up = bar["high"] > or_high
            hit_dn = bar["low"] < or_low
            if hit_up and hit_dn:
                # Both sides crossed within one minute; use open to guess order.
                if bar["open"] >= or_high:
                    direction = "U"
                elif bar["open"] <= or_low:
                    direction = "D"
                else:
                    direction = "U" if bar["close"] >= bar["open"] else "D"
            elif hit_up:
                direction = "U"
            elif hit_dn:
                direction = "D"

            if direction == "U":
                breakout_price = or_high
                breakout_at = bar["time"]
                breakout_bar = bar
            elif direction == "D":
                breakout_price = or_low
                breakout_at = bar["time"]
                breakout_bar = bar

        # After (or same bar as) breakout, evaluate staged levels.
        if direction == "U":
            if pt1_at is None and bar["high"] >= pt1_up:
                pt1_at = bar["time"]
            if pt2_at is None and bar["high"] >= pt2_up:
                pt2_at = bar["time"]
            if reversed_at is None and bar["low"] <= or_low and bar["time"] > breakout_at:
                reversed_at = bar["time"]
        elif direction == "D":
            if pt1_at is None and bar["low"] <= pt1_dn:
                pt1_at = bar["time"]
            if pt2_at is None and bar["low"] <= pt2_dn:
                pt2_at = bar["time"]
            if reversed_at is None and bar["high"] >= or_high and bar["time"] > breakout_at:
                reversed_at = bar["time"]

    if direction is None:
        return None
    return {
        "direction":          direction,
        "breakout_price":     breakout_price,
        "breakout_at":        breakout_at,
        "pt1_hit_at":         pt1_at,
        "pt2_hit_at":         pt2_at,
        "reversed_at":        reversed_at,
        "breakout_bar_open":  breakout_bar["open"]  if breakout_bar else None,
        "breakout_bar_high":  breakout_bar["high"]  if breakout_bar else None,
        "breakout_bar_low":   breakout_bar["low"]   if breakout_bar else None,
        "breakout_bar_close": breakout_bar["close"] if breakout_bar else None,
    }


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def _upsert_or(cur, trade_date: date, stock_id: str, or_high: float, or_low: float):
    cur.execute(
        """
        INSERT INTO tw.intraday_opening_range
            (trade_date, stock_id, or_high, or_low, or_range, established_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (trade_date, stock_id) DO UPDATE SET
            or_high        = EXCLUDED.or_high,
            or_low         = EXCLUDED.or_low,
            or_range       = EXCLUDED.or_range,
            established_at = NOW()
        """,
        (trade_date, stock_id, or_high, or_low, or_high - or_low),
    )


def _upsert_signal(cur, trade_date: date, stock_id: str, sig: dict):
    cur.execute(
        """
        INSERT INTO tw.intraday_orb_signals
            (trade_date, stock_id, direction, breakout_price, breakout_at,
             pt1_hit_at, pt2_hit_at, reversed_at,
             breakout_bar_open, breakout_bar_high, breakout_bar_low,
             breakout_bar_close, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (trade_date, stock_id) DO UPDATE SET
            direction          = EXCLUDED.direction,
            breakout_price     = EXCLUDED.breakout_price,
            breakout_at        = EXCLUDED.breakout_at,
            pt1_hit_at         = EXCLUDED.pt1_hit_at,
            pt2_hit_at         = EXCLUDED.pt2_hit_at,
            reversed_at        = EXCLUDED.reversed_at,
            breakout_bar_open  = EXCLUDED.breakout_bar_open,
            breakout_bar_high  = EXCLUDED.breakout_bar_high,
            breakout_bar_low   = EXCLUDED.breakout_bar_low,
            breakout_bar_close = EXCLUDED.breakout_bar_close,
            updated_at         = NOW()
        """,
        (
            trade_date,
            stock_id,
            sig["direction"],
            sig["breakout_price"],
            sig["breakout_at"],
            sig["pt1_hit_at"],
            sig["pt2_hit_at"],
            sig["reversed_at"],
            sig["breakout_bar_open"],
            sig["breakout_bar_high"],
            sig["breakout_bar_low"],
            sig["breakout_bar_close"],
        ),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    init_db()

    if len(argv) == 0:
        dates = _maybe_add_today(_recent_trading_days(_DEFAULT_LOOKBACK))
    elif len(argv) == 1:
        try:
            n = int(argv[0])
            dates = _maybe_add_today(_recent_trading_days(n))
        except ValueError:
            print(f"[ERROR] single argument must be an integer lookback, got: {argv[0]}")
            return 1
    elif len(argv) == 2:
        start = date.fromisoformat(argv[0])
        end = date.fromisoformat(argv[1])
        dates = _trading_days_in_range(start, end)
    else:
        print("Usage: python backfill_orb.py [lookback_days | start_date end_date]")
        return 1

    if not dates:
        print("[ERROR] no trading days matched the requested window")
        return 1

    target_set = set(dates)
    start_date = dates[0]
    end_date = dates[-1]
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    # Build watchlist using latest date's universe (fund holdings / ETF holdings
    # move slowly, so using the current snapshot for historical dates is
    # good enough for initial calibration).
    watchlist = build_orb_watchlist(end_date)
    print(f"Backfilling ORB for {len(watchlist)} stocks × {len(dates)} days "
          f"({start_str} .. {end_str})")

    api = load_api()
    stats = {
        "or_written":       0,
        "signals_written":  0,
        "kbar_empty":       0,
        "contract_missing": 0,
        "errors":           0,
    }

    try:
        for i, code in enumerate(watchlist, 1):
            try:
                contract = _find_contract(api, code)
                if contract is None:
                    stats["contract_missing"] += 1
                    continue

                kbars = api.kbars(contract, start=start_str, end=end_str)
                bars_by_date = _group_bars_by_date(kbars)
                if not bars_by_date:
                    stats["kbar_empty"] += 1
                    continue

                with get_cursor() as cur:
                    for td in target_set:
                        bars = bars_by_date.get(td)
                        if not bars:
                            continue
                        or_res = _compute_opening_range(bars)
                        if or_res is None:
                            continue
                        or_high, or_low = or_res
                        _upsert_or(cur, td, code, or_high, or_low)
                        stats["or_written"] += 1

                        sig = _replay_signals(bars, or_high, or_low)
                        if sig is not None:
                            _upsert_signal(cur, td, code, sig)
                            stats["signals_written"] += 1

            except Exception:
                stats["errors"] += 1
                print(f"[ERROR] {code}:")
                traceback.print_exc()

            if i % 10 == 0 or i == len(watchlist):
                print(f"  {i}/{len(watchlist)} stocks processed  "
                      f"OR={stats['or_written']} SIG={stats['signals_written']}")
            time.sleep(_INTER_CALL_SLEEP)

    finally:
        logout_api(api)

    print()
    print(f"Done.")
    print(f"  OR rows written:       {stats['or_written']}")
    print(f"  Signal rows written:   {stats['signals_written']}")
    print(f"  Contracts not found:   {stats['contract_missing']}")
    print(f"  Empty kbar responses:  {stats['kbar_empty']}")
    print(f"  Errors:                {stats['errors']}")
    return 0 if stats["errors"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
