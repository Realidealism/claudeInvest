"""Margin short-sale constraints for the short side (user ruling 2026-10-20).

A short sold on margin (融券) must be bought back by the 融券最後回補日, which
the exchanges publish as 停券起日, and none can be sold from 停券起日 to
停券迄日. The official boards (TWSE BFI84U / BFI84U2, TPEx term) list both;
tw.short_cover_calendar holds them, history included. Rows derived from AGM
dates (mops_agm_derived) are not used: they run one trading day early.
An extraordinary shareholders' meeting (臨時會) does not force a cover
(融資融券業務操作辦法 §76).

Arms: margin puts every short on margin; margin_futures shorts through stock
futures where they were usable, which neither 停券 nor a cover date touches.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date

import numpy as np

OFFICIAL_SOURCES = ("BFI84U", "BFI84U2", "tpex_term", "tpex_term_hist")
NO_COVER_REASONS = ("臨時會",)
# 停止融券賣出四日 (§76) where the board gives no 停券迄日
DEFAULT_BLOCK_BARS = 4
# Stock futures are usable when the standard contracts traded at least this
# many lots a day on average over the window of trading days before
FUTURES_MIN_LOTS = 50
FUTURES_WINDOW = 20

Event = tuple[date, date | None]   # (停券起日 = 最後回補日, 停券迄日)


def load_calendar() -> dict[str, list[Event]]:
    """{stock_id: [(last_cover_date, suspension_end), ...]} from the official
    rows; read-only."""
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        cur.execute(
            """SELECT DISTINCT stock_id, last_cover_date, suspension_end
               FROM tw.short_cover_calendar
               WHERE source = ANY(%s) AND NOT (reason = ANY(%s))
               ORDER BY stock_id, last_cover_date""",
            (list(OFFICIAL_SOURCES), list(NO_COVER_REASONS)),
        )
        out: dict[str, list[Event]] = {}
        for r in cur.fetchall():
            out.setdefault(r["stock_id"], []).append((r["last_cover_date"], r["suspension_end"]))
    return out


def load_futures_availability() -> dict[str, set[date]]:
    """{stock_id: dates its stock futures were usable}: the standard
    contracts' day-session volume (spread rows out), all months summed,
    averaged >= FUTURES_MIN_LOTS over the FUTURES_WINDOW trading days before,
    the day itself excluded (user ruling 2026-10-20). A trading day without a
    row counts as 0; trading days are TAIEX's and TAIFEX's together. Read-only
    (tw.taifex_futures_daily, tw.stock_futures_contracts)."""
    import pandas as pd
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        cur.execute(
            """SELECT c.stock_id, f.trade_date, SUM(COALESCE(f.volume, 0)) AS volume
               FROM tw.taifex_futures_daily f
               JOIN tw.stock_futures_contracts c ON c.contract = f.contract
               WHERE c.kind = 'stock' AND c.standard AND f.session = '一般'
                 AND f.contract_month NOT LIKE '%%/%%'
               GROUP BY c.stock_id, f.trade_date""")
        vol = pd.DataFrame(cur.fetchall(), columns=["stock_id", "trade_date", "volume"])
        cur.execute("SELECT trade_date FROM tw.index_prices WHERE index_id = 'TAIEX'")
        taiex = {r["trade_date"] for r in cur.fetchall()}
        cur.execute("SELECT DISTINCT trade_date FROM tw.taifex_futures_daily WHERE session = '一般'")
        taifex = {r["trade_date"] for r in cur.fetchall()}
        cur.execute("SELECT MAX(trade_date) AS d FROM tw.daily_prices")
        prices_last = cur.fetchone()["d"]
    _check_futures_feed(set(vol["trade_date"]), taiex, prices_last)
    return availability_from(vol, sorted(taiex | taifex))


def _check_futures_feed(have: set[date], taiex: set[date], prices_last: date | None) -> None:
    """A missing day reads as no futures traded, which quietly puts shorts on
    margin; fail instead when the stock futures stop before the daily prices
    or skip a trading day."""
    if not have:
        raise RuntimeError("no stock futures rows (tw.taifex_futures_daily / tw.stock_futures_contracts)")
    first, last = min(have), max(have)
    gaps = sorted(d for d in taiex if first <= d <= last and d not in have)
    if gaps or (prices_last is not None and last < prices_last):
        raise RuntimeError(f"stock futures feed incomplete: last day {last} (daily prices to {prices_last}), "
                           f"trading days without rows {[str(d) for d in gaps[:10]]}"
                           f"{' ...' if len(gaps) > 10 else ''}")


def availability_from(vol, days: list[date]) -> dict[str, set[date]]:
    """The rule of load_futures_availability on a (stock_id, trade_date,
    volume) frame of standard day-session volume and the trading days."""
    if vol.empty:
        return {}
    v = vol.pivot(index="trade_date", columns="stock_id", values="volume").astype(float)
    v = v.reindex(days).fillna(0.0)
    avg = v.shift(1).rolling(FUTURES_WINDOW, min_periods=FUTURES_WINDOW).mean()
    usable = avg >= FUTURES_MIN_LOTS
    return {sid: set(usable.index[usable[sid].to_numpy()]) for sid in usable.columns if usable[sid].any()}


def futures_exempt(dates: list[date], futures_days: set[date]) -> np.ndarray:
    """Bars on which a short could go through stock futures instead of margin:
    no 停券 block there, and a position opened there is never force-covered."""
    return np.fromiter((d in futures_days for d in dates), dtype=np.bool_, count=len(dates))


def masks(dates: list[date], events: list[Event]) -> tuple[np.ndarray, np.ndarray]:
    """(force_exit, entry_block) over the stock's bars.

    force_exit marks the bar on the last cover date, or the first bar after
    it when the stock did not trade that day: a market closure moves the
    cover one day later (TWSE Q&A), and a halted stock can only be bought
    back when it trades again; events past the last bar are ignored.
    entry_block marks the bars from 停券起日 to 停券迄日, or DEFAULT_BLOCK_BARS
    bars from 停券起日 when the board gives no end, and always the cover bar."""
    n = len(dates)
    force_exit = np.zeros(n, dtype=np.bool_)
    entry_block = np.zeros(n, dtype=np.bool_)
    if not n:
        return force_exit, entry_block
    for start, end in events:
        if start > dates[-1]:
            continue
        k = bisect_left(dates, start)
        force_exit[k] = True
        hi = bisect_right(dates, end) if end is not None else k + DEFAULT_BLOCK_BARS
        entry_block[k:max(hi, k + 1)] = True
    return force_exit, entry_block
