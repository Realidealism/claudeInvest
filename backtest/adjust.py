"""
Back-adjustment factors for corporate actions (ex-rights/dividends, capital
reductions), so a series can be read as total return.

Each event on bar T has a price factor f and a volume factor f_v that apply to
every bar before T; the cumulative factor of bar i is the product over the
events after i, so the latest bar keeps its raw price.

Sources, in order (one event per bar and source class):
  - tw.dividends: cash (NTD/share), stock (NTD of par per share, /10 = shares)
  - tw.dividend_calendar: rows not in tw.dividends; stock_ratio is shares per
    share already
  - tw.capital_changes REDUCTION: ratio = close before / reference price after;
    applied only where the resumption bar's ref (or open) shows that step
    (some rows do not: 3018 2023-11-11 ratio 0.5, no move in the prices)
  - the exchange reference price, for a drop no table explains: only when the
    previous bar is the previous trading day, ref < 0.995 * previous close,
    ref != close and no bar in T-1..T+1 has the ref == close defect.
A jump after a halt that no table explains is left alone (the >40% blackout
in signal_backtest.factories._conditions still covers it).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np

from db.connection import get_cursor

REF_DROP_MIN = 0.005          # ref below the previous close by more than this
REF_FACTOR_FLOOR = 0.5        # a ref-implied factor below this is not trusted
# Reduction row vs the resumption bar: the close before a long halt can be
# stale by a few percent (1418 2017-11-28 5.3%, 4806 2025-10-07 5.8%); the
# open can also sit up to 10% off the ref.
CAPITAL_REF_TOL = 0.06
CAPITAL_OPEN_TOL = 0.15
LOSS_NOTES = ("彌補虧損", "Making up losses")
CASH_NOTES = ("現金減資", "Cash refund")

# Reference prices known to be wrong over a range (not the ref == close shape).
REF_DEFECTS: dict[str | None, list[tuple[date, date]]] = {
    "6423": [(date(2024, 5, 17), date(2026, 1, 22))],   # ref = close two days back
}
# Whole-market days with a wrong reference price (market as in tw.stocks).
REF_DEFECT_DAYS: dict[str, set[date]] = {
    "TWSE": {date(2020, 12, 16)},
}

_TRADING_DAYS: list[date] | None = None


@dataclass
class AdjustEvent:
    bar: int                  # index of the first bar on the new basis
    source: str               # dividends / calendar / capital / ref
    price_factor: float
    volume_factor: float


@dataclass
class RefCandidate:
    """A reference-price drop that no table explains, or a capital-reduction
    row the prices do not show, and what became of it."""
    bar: int
    ratio: float              # ref / previous close; for a reduction row, its ratio
    outcome: str              # used / contaminated / known_defect / out_of_range / not_contiguous
                              # / capital_mismatch / capital_duplicate


def _trading_days() -> list[date]:
    global _TRADING_DAYS
    if _TRADING_DAYS is None:
        with get_cursor(commit=False) as cur:
            cur.execute("SELECT trade_date FROM tw.index_prices WHERE index_id = 'TAIEX' "
                        "AND close_price IS NOT NULL ORDER BY trade_date")
            _TRADING_DAYS = [r["trade_date"] for r in cur.fetchall()]
    return _TRADING_DAYS


def _fetch_events(stock_id: str) -> tuple[list[dict], list[dict], list[dict], str | None]:
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT ex_date, coalesce(cash_dividend, 0) cash, coalesce(stock_dividend, 0) sd "
                    "FROM tw.dividends WHERE stock_id = %s ORDER BY ex_date", (stock_id,))
        divs = cur.fetchall()
        cur.execute("SELECT ex_date, coalesce(cash_dividend, 0) cash, coalesce(stock_ratio, 0) ratio "
                    "FROM tw.dividend_calendar WHERE stock_id = %s ORDER BY ex_date", (stock_id,))
        cal = cur.fetchall()
        cur.execute("SELECT effective_date, ratio, note FROM tw.capital_changes "
                    "WHERE stock_id = %s AND event_type = 'REDUCTION' ORDER BY effective_date", (stock_id,))
        cap = cur.fetchall()
        cur.execute("SELECT market FROM tw.stocks WHERE stock_id = %s", (stock_id,))
        row = cur.fetchone()
    return divs, cal, cap, (row["market"] if row else None)


def compute_events(stock_id: str, rows: list[dict], divs: list[dict], cal: list[dict],
                   cap: list[dict], market: str | None,
                   trading_days: list[date]) -> tuple[list[AdjustEvent], list[RefCandidate]]:
    """Events on the given rows (ascending by trade_date, raw prices)."""
    dates = [r["trade_date"] for r in rows]
    close = [float(r["close_price"]) for r in rows]
    ref = [None if r["ref_price"] is None else float(r["ref_price"]) for r in rows]
    n = len(rows)
    events: list[AdjustEvent] = []
    taken: set[int] = set()
    bar_days = np.array(dates, dtype="datetime64[D]")

    def bar_on_or_after(d: date) -> int | None:
        k = int(np.searchsorted(bar_days, np.datetime64(d)))
        return k if 0 < k < n else None   # an event on the first bar has no bar before it

    def dividend(k: int, cash: float, shares: float, source: str) -> None:
        p = close[k - 1]
        f = (p - cash) / (1 + shares) / p
        events.append(AdjustEvent(k, source, f, 1 + shares))
        taken.add(k)

    for d in divs:
        cash, sd = float(d["cash"]), float(d["sd"])
        if cash <= 0 and sd <= 0:
            continue
        k = bar_on_or_after(d["ex_date"])
        if k is not None:
            dividend(k, cash, sd / 10, "dividends")
    for d in cal:
        cash, ratio = float(d["cash"]), float(d["ratio"])
        if cash <= 0 and ratio <= 0:
            continue
        k = bar_on_or_after(d["ex_date"])
        # a bar tw.dividends already covers keeps that row
        if k is not None and k not in taken:
            dividend(k, cash, ratio, "calendar")
    candidates: list[RefCandidate] = []
    for c in cap:
        k = bar_on_or_after(c["effective_date"])
        if k is None:
            continue
        ratio = float(c["ratio"])
        if any(e.bar == k and e.source == "capital" for e in events):
            candidates.append(RefCandidate(k, ratio, "capital_duplicate"))   # 2327 2022-10-31
            continue
        p = close[k - 1]
        # A row the prices do not show is not applied: the bar's ref (or open)
        # must sit near the previous close divided by the ratio.
        expected = p / ratio
        if ref[k] is not None:
            fits = abs(expected / ref[k] - 1) <= CAPITAL_REF_TOL
        else:
            fits = abs(expected / float(rows[k]["open_price"]) - 1) <= CAPITAL_OPEN_TOL
        if not fits:
            candidates.append(RefCandidate(k, ratio, "capital_mismatch"))
            continue
        # The ratio is close before the halt / ref after it, so it already
        # holds a dividend that went ex while halted: such a dividend keeps
        # only its share count (6197 2017-10-02, 2327 2022-10-31).
        for e in events:
            if e.bar == k and e.source in ("dividends", "calendar"):
                e.price_factor = 1.0
        vf = ratio
        if c["note"] in CASH_NOTES:
            # ref = (P - 10r) / (1 - r) with ref = P / ratio  ->  r = (ref - P) / (ref - 10);
            # below par the ref falls (8201 2016-07-18), above it rises
            ref_after = p / ratio
            r = (ref_after - p) / (ref_after - 10) if ref_after != 10 else 0.0
            if 0 < r < 1:
                vf = 1 - r
        events.append(AdjustEvent(k, "capital", 1 / ratio, vf))
        taken.add(k)

    # Reference-price drops no table explains.
    td_index = {d: i for i, d in enumerate(trading_days)}
    bad = [False] * n
    for i in range(1, n):
        bad[i] = ref[i] is not None and ref[i] == close[i] and close[i] != close[i - 1]
    defects = REF_DEFECTS.get(stock_id, [])
    defect_days = REF_DEFECT_DAYS.get(market or "", set())
    for k in range(1, n):
        if k in taken or ref[k] is None or ref[k] == close[k]:
            continue
        p = close[k - 1]
        ratio = ref[k] / p
        if ratio >= 1 - REF_DROP_MIN:
            continue
        a, b = td_index.get(dates[k - 1]), td_index.get(dates[k])
        if a is None or b is None or b != a + 1:
            outcome = "not_contiguous"
        elif any(bad[j] for j in (k - 1, k, k + 1) if 0 <= j < n):
            outcome = "contaminated"
        elif dates[k] in defect_days or any(lo <= dates[k] <= hi for lo, hi in defects):
            outcome = "known_defect"
        elif ratio < REF_FACTOR_FLOOR:
            outcome = "out_of_range"
        else:
            outcome = "used"
            events.append(AdjustEvent(k, "ref", ratio, 1.0))
        candidates.append(RefCandidate(k, ratio, outcome))
    events.sort(key=lambda e: e.bar)
    return events, candidates


def cumulative(n: int, events: list[AdjustEvent]) -> tuple[np.ndarray, np.ndarray]:
    """Per-bar cumulative price and volume factors (latest bar = 1)."""
    pe = np.ones(n)
    ve = np.ones(n)
    for e in events:
        pe[e.bar] *= e.price_factor
        ve[e.bar] *= e.volume_factor
    # factor of bar i = product of event factors on bars i+1 .. n-1
    pf = np.ones(n)
    vf = np.ones(n)
    pf[:-1] = np.cumprod(pe[::-1])[::-1][1:]
    vf[:-1] = np.cumprod(ve[::-1])[::-1][1:]
    return pf, vf


def adjustment(stock_id: str, rows: list[dict]) -> tuple[np.ndarray, np.ndarray, list[AdjustEvent]]:
    """Cumulative (price_factor, volume_factor) for the rows, and the events used."""
    divs, cal, cap, market = _fetch_events(stock_id)
    events, _ = compute_events(stock_id, rows, divs, cal, cap, market, _trading_days())
    pf, vf = cumulative(len(rows), events)
    return pf, vf, events
