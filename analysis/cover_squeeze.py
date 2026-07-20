"""股東會強制回補軋空 (AGM forced-cover squeeze) daily candidate ranking.

Heavily-shorted stocks must force-cover their 融券 before a stock's 股東會
book-closure. The last-cover date (融券最後回補日) sits ~2 months before the
meeting, so the squeeze season is Feb–Apr, not the June meeting month. In the
~6 trading days INTO the cover date, high days-to-cover names squeeze up
(validated: W=6 high-dtc net +0.78%, top-decile +1.11%, +8% stop PF 1.49).

days-to-cover (dtc) = 融券今日餘額(shares) / avg daily volume(shares). Only
融券 (margin short) is forced to cover before AGM — SBL/借券 is not — so this
uses tw.daily_prices.short_balance, not sbl_balance.

This module only RANKS today's live candidates for the standalone daily list
(export.generate → cover_squeeze.json). The tradeable edge itself is already
validated in memory project_agm_forced_cover_squeeze; this is not a backtester.
"""

from __future__ import annotations

from datetime import date, timedelta

# Validated study parameters, surfaced so the frontend can show the operating rule.
# dtc thresholds are ABSOLUTE (tmp/_agm_squeeze_backtest.py, 2026-07-20): absolute
# dtc≥0.5 beat cross-sectional top-decile (W=6 net PF 1.698 / +1.258% vs 1.487 /
# +1.114%); the bottom-tercile control LOST money, so names below the floor have no
# edge and are dropped from the list.
WINDOW_TD = 6            # squeeze plays out in the ~6 trading days into the cover date
FORWARD_CAL_DAYS = 9     # ~6 trading days in calendar days (weekend padding)
VOL_WIN_DAYS = 21        # trailing 21 trading days for the dtc denominator (matches backtest)
STOP_PCT = 8.0           # validated protective stop
FLOOR_DTC = 0.3          # list-inclusion floor (edge floor: PF 1.579 / +1.140%)
STRONG_DTC = 0.5         # ★ strong signal (sweet spot: PF 1.698 / +1.258%)


def _latest_trade_date(cur, as_of: date | None) -> date | None:
    if as_of is None:
        cur.execute("SELECT MAX(trade_date) m FROM tw.daily_prices")
    else:
        cur.execute(
            "SELECT MAX(trade_date) m FROM tw.daily_prices WHERE trade_date <= %s",
            (as_of,),
        )
    row = cur.fetchone()
    return row["m"] if row else None


def _position_states(cur, asof: date, tickers: list[str]):
    """Return f(stock_id) -> unified-strategy operation state as of `asof`:
    'long'/'short' (open position), 'exited_long'/'exited_short' (closed that day,
    keeping the direction), 'flat' (tracked, no position), 'na' (position tracking
    not yet recorded on that date — tw.open_positions starts 2026-04-28; must not
    read as 空手)."""
    cur.execute(
        "SELECT MAX(snapshot_date) d FROM tw.open_positions WHERE snapshot_date <= %s",
        (asof,),
    )
    row = cur.fetchone()
    pos_date = row["d"] if row else None
    if pos_date is None:
        return lambda _sid: "na"
    cur.execute(
        """SELECT stock_id, side, is_exited FROM tw.open_positions
           WHERE snapshot_date = %s AND stock_id = ANY(%s)""",
        (pos_date, tickers),
    )
    state: dict[str, str] = {}
    for r in cur.fetchall():
        s = ("exited_" + r["side"]) if r["is_exited"] else r["side"]
        # prefer an open position over an exited one for the same stock
        prev = state.get(r["stock_id"])
        if prev is None or prev.startswith("exited"):
            state[r["stock_id"]] = s
    return lambda sid: state.get(sid, "flat")


def rank_candidates(cur, as_of: date | None = None) -> dict:
    """Rank the AGM-cover-squeeze candidates live as of `as_of` (default: latest
    trade date). Returns a snapshot dict ready for JSON serialisation.

    A candidate is a stock whose next 股東會 融券最後回補日 falls within the coming
    ~6 trading days (so it is entering / inside the squeeze window) and that still
    carries a 融券 balance. Ranked by dtc; the top decile is flagged is_high_dtc
    (the validated actionable subset)."""
    asof = _latest_trade_date(cur, as_of)
    if asof is None:
        return {"as_of": None, "window_end": None, "off_season": True,
                "candidates": [], "next_cover_date": None, "params": _params()}

    win_end = asof + timedelta(days=FORWARD_CAL_DAYS)

    # Nearest upcoming 股東會 cover date per stock; dtc as of `asof` with the
    # trailing 21-trading-day avg volume (exact rows, not a calendar approximation,
    # so the absolute dtc threshold lines up with the backtest denominator).
    cur.execute(
        """
        WITH nxt AS (
            SELECT DISTINCT ON (stock_id) stock_id, last_cover_date, meeting_date
            FROM tw.short_cover_calendar
            WHERE reason = %s AND last_cover_date > %s AND last_cover_date <= %s
            ORDER BY stock_id, last_cover_date
        ),
        av AS (
            SELECT stock_id, AVG(volume) av_vol
            FROM (
                SELECT stock_id, volume,
                       ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY trade_date DESC) rn
                FROM tw.daily_prices
                WHERE trade_date <= %s
                  AND trade_date > (%s::date - INTERVAL '60 days')
                  AND volume > 0
                  AND stock_id IN (SELECT stock_id FROM nxt)
            ) t
            WHERE rn <= %s
            GROUP BY stock_id
        )
        SELECT n.stock_id, st.name, st.market,
               n.last_cover_date, n.meeting_date,
               dp.short_balance, dp.close_price, av.av_vol,
               dp.short_balance::float / NULLIF(av.av_vol, 0) AS dtc
        FROM nxt n
        JOIN tw.daily_prices dp ON dp.stock_id = n.stock_id AND dp.trade_date = %s
        JOIN av ON av.stock_id = n.stock_id
        LEFT JOIN tw.stocks st ON st.stock_id = n.stock_id
        WHERE dp.short_balance > 0 AND av.av_vol > 0
          AND dp.short_balance::float / NULLIF(av.av_vol, 0) >= %s
        ORDER BY dtc DESC
        """,
        ("股東會", asof, win_end, asof, asof, VOL_WIN_DAYS, asof, FLOOR_DTC),
    )
    rows = cur.fetchall()

    if not rows:
        cur.execute(
            """SELECT MIN(last_cover_date) d FROM tw.short_cover_calendar
               WHERE reason = %s AND last_cover_date > %s""",
            ("股東會", asof),
        )
        nxt = cur.fetchone()
        return {"as_of": asof, "window_end": win_end, "off_season": True,
                "candidates": [], "next_cover_date": nxt["d"] if nxt else None,
                "params": _params()}

    # Technical-side operation state (統一策略持倉) as of `asof`, joined per stock.
    # "flat" = tracked-but-no-position; "na" = position tracking not yet recorded on
    # that date (tw.open_positions only starts 2026-04-28) — must not read as 空手.
    tickers = [r["stock_id"] for r in rows]
    pos_state = _position_states(cur, asof, tickers)

    candidates = []
    for rank, r in enumerate(rows, start=1):
        candidates.append({
            "rank": rank,
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "last_cover_date": r["last_cover_date"],
            "meeting_date": r["meeting_date"],
            "days_to_cover_date": (r["last_cover_date"] - asof).days,
            "short_balance": r["short_balance"],
            "avg_volume": int(r["av_vol"]),
            "dtc": round(r["dtc"], 3),
            "close": r["close_price"],
            "position_state": pos_state(r["stock_id"]),
            "is_strong": r["dtc"] >= STRONG_DTC,
        })

    return {"as_of": asof, "window_end": win_end, "off_season": False,
            "candidates": candidates, "next_cover_date": None, "params": _params()}


def _params() -> dict:
    return {"window_td": WINDOW_TD, "vol_win_days": VOL_WIN_DAYS,
            "stop_pct": STOP_PCT, "floor_dtc": FLOOR_DTC, "strong_dtc": STRONG_DTC}


if __name__ == "__main__":
    import sys
    from db.connection import get_cursor

    arg = sys.argv[1] if len(sys.argv) > 1 else None
    as_of = date.fromisoformat(arg) if arg else None
    with get_cursor(commit=False) as cur:
        snap = rank_candidates(cur, as_of)
    print(f"as_of={snap['as_of']} off_season={snap['off_season']} "
          f"n={len(snap['candidates'])} next_cover={snap['next_cover_date']}")
    for c in snap["candidates"][:20]:
        flag = "★" if c["is_strong"] else " "
        print(f"{flag} #{c['rank']:2d} {c['ticker']:6s} {(c['name'] or ''):10s} "
              f"cover={c['last_cover_date']} d={c['days_to_cover_date']:+d} "
              f"dtc={c['dtc']:.3f} pos={c['position_state']} close={c['close']}")
