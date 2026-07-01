"""麻紗 SOP 三步 + 撐壓辨識 (MASHA_FUTURES_SPEC §4, §5, M3 subset).

SOP is the core 扣板機 (§4.1): 站上 level → 回踩 level 附近未有效跌破 → 吞噬K.
Levels are horizontal 撐壓 where swing pivots cluster (§5.1/§5.2). Tolerances are
parameters (swept later). Semi-auto (◐ per §10).
"""
from __future__ import annotations

from typing import Optional, Sequence

from broker.types import Bar
from strategy.signals_masha import engulf


# ── §5 撐壓辨識 ─────────────────────────────────────────────────────────

def pivots(bars: Sequence[Bar], k: int = 2) -> tuple[list[float], list[float]]:
    """Swing highs/lows: a bar whose high (low) is the extreme of ±k neighbours.
    Only confirmed pivots (needs k bars on each side)."""
    highs: list[float] = []
    lows: list[float] = []
    for i in range(k, len(bars) - k):
        window = bars[i - k:i + k + 1]
        if all(bars[i].high >= b.high for b in window):
            highs.append(bars[i].high)
        if all(bars[i].low <= b.low for b in window):
            lows.append(bars[i].low)
    return highs, lows


def sr_levels(bars: Sequence[Bar], k: int = 2, tol: float = 20.0,
              min_touches: int = 2) -> list[float]:
    """Horizontal 撐壓 levels where ≥min_touches swing pivots cluster within tol."""
    pts = sorted(sum(pivots(bars, k), []))
    clusters: list[list[float]] = []
    for p in pts:
        for c in clusters:
            if abs(sum(c) / len(c) - p) <= tol:
                c.append(p)
                break
        else:
            clusters.append([p])
    return [sum(c) / len(c) for c in clusters if len(c) >= min_touches]


# ── §4.1 SOP 三步 ──────────────────────────────────────────────────────

def check_sop(bars: Sequence[Bar], level: float, direction: str,
              tol: float = 0.0, window: int = 10) -> bool:
    """站上 → 回踩 → 吞噬 (§4.1) around `level`, completing on the current bar.
      step1: an earlier bar closes through level (多:向上 / 空:向下)
      step2: a later bar retests level (回踩附近) without 有效跌破/突破 (close-break)
      step3: current bar is an engulf in `direction`
    """
    if len(bars) < 3:
        return False
    e = engulf(bars)
    if e != direction:                      # step3
        return False
    broke = retested = False
    for b in bars[-window:-1]:              # bars before the current (step1, step2)
        if direction == "long":
            if not broke and b.close > level + tol:
                broke = True
            elif broke and b.low <= level + tol and b.close >= level - tol:
                retested = True
        else:
            if not broke and b.close < level - tol:
                broke = True
            elif broke and b.high >= level - tol and b.close <= level + tol:
                retested = True
    return broke and retested


def sop_signal(bars: Sequence[Bar], k: int = 2, level_tol: float = 20.0,
               retest_tol: float = 0.0, window: int = 10,
               hist: int = 60) -> Optional[str]:
    """Scan recent 撐壓 levels for a completed SOP on the current bar. `hist`
    caps the history so pivot detection stays O(hist), not O(all bars)."""
    recent = bars[-hist:]
    for lv in sr_levels(recent, k, level_tol):
        if check_sop(recent, lv, "long", retest_tol, window):
            return "long"
        if check_sop(recent, lv, "short", retest_tol, window):
            return "short"
    return None
