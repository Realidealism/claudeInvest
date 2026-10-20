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
from pathlib import Path

import numpy as np

OFFICIAL_SOURCES = ("BFI84U", "BFI84U2", "tpex_term", "tpex_term_hist")
NO_COVER_REASONS = ("臨時會",)
# 停止融券賣出四日 (§76) where the board gives no 停券迄日
DEFAULT_BLOCK_BARS = 4
# Experiment input for the margin_futures arm (not yet a DB table)
FUTURES_AVAILABILITY = (Path(__file__).resolve().parents[1]
                        / "tmp/short_sale_research/futures/availability.parquet")

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


def load_futures_availability(path=FUTURES_AVAILABILITY) -> dict[str, set[date]]:
    """{stock_id: dates its stock futures were usable}: the standard contract's
    day-session volume, all months summed, averaged >= 50 lots over the 20
    trading days before (user ruling 2026-10-20). Built offline from TAIFEX
    daily data (tmp/short_sale_research/futures/)."""
    import pandas as pd
    df = pd.read_parquet(path)
    df = df[df["avail"]]
    return {sid: set(g["trade_date"]) for sid, g in df.groupby("stock_id")}


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
