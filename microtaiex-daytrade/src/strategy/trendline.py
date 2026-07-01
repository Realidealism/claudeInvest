"""麻紗 軌道線 (MASHA_FUTURES_SPEC §3, M4 subset) — 逸安法擬合.

§3.1 畫線 SOP: 上升趨勢連 swing 低點、下降趨勢連 swing 高點，再平移複製成通道
(上軌/下軌)。這裡用最小平方法擬合 swing pivots (逸安法的機械近似)，並以「多少
pivots 落在線上 (容差內)」驗證合理性 (§3.2 有效觸碰)。

軌道線是規格主進場依據 (§2.1 定方向 + §3.4 順逆勢)。輸出:
  direction: "up" | "down" | "range"
  lower / upper: 當前 K 的下軌 / 上軌價位 (range → None)

半自動 ◐ (§10): 擬合窗口/容差/最少觸碰數皆為可掃描參數.
"""
from __future__ import annotations

from typing import Optional, Sequence

from broker.types import Bar
from core import clock
from strategy.signals_masha import engulf, doji_flip, island_reversal

# §3.4 rail 扣板機 candidates (all reversal-at-location, return "long"/"short").
_RAIL_TRIGGERS = {"engulf": engulf, "doji": doji_flip, "island": island_reversal}


def _fit_line(points: Sequence[tuple[float, float]]) -> Optional[tuple[float, float]]:
    """Least-squares (slope, intercept) through (x, y) points; None if degenerate."""
    n = len(points)
    if n < 2:
        return None
    mx = sum(p[0] for p in points) / n
    my = sum(p[1] for p in points) / n
    denom = sum((p[0] - mx) ** 2 for p in points)
    if denom == 0:
        return None
    slope = sum((p[0] - mx) * (p[1] - my) for p in points) / denom
    return slope, my - slope * mx


def _indexed_pivots(bars: Sequence[Bar], k: int
                    ) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Swing highs/lows carrying their bar index as x (for line fitting)."""
    highs: list[tuple[int, float]] = []
    lows: list[tuple[int, float]] = []
    for i in range(k, len(bars) - k):
        window = bars[i - k:i + k + 1]
        if all(bars[i].high >= b.high for b in window):
            highs.append((i, bars[i].high))
        if all(bars[i].low <= b.low for b in window):
            lows.append((i, bars[i].low))
    return highs, lows


def _touches(line: Optional[tuple[float, float]],
             pts: Sequence[tuple[float, float]], tol: float) -> int:
    """How many pivots lie within tol of the fitted line (§3.2 有效觸碰)."""
    if line is None:
        return 0
    s, b = line
    return sum(1 for (x, y) in pts if abs(y - (s * x + b)) <= tol)


def channel(bars: Sequence[Bar], k: int = 2, lookback: int = 60,
            tol: float = 30.0, min_touches: int = 3
            ) -> tuple[str, Optional[float], Optional[float]]:
    """Fit an up/down trend channel over the recent `lookback` bars.

    Up-channel: line through swing LOWS (下軌 support) with slope > 0.
    Down-channel: line through swing HIGHS (上軌 resistance) with slope < 0.
    Direction picks the better-validated slope-consistent side; else "range".
    Returns (direction, lower_at_now, upper_at_now); range → (·, None, None).
    """
    w = bars[-lookback:]
    if len(w) < 2 * k + 2:
        return "range", None, None
    highs, lows = _indexed_pivots(w, k)
    up = _fit_line(lows)          # support line (through lows)
    dn = _fit_line(highs)         # resistance line (through highs)
    up_ok = up is not None and up[0] > 0 and _touches(up, lows, tol) >= min_touches
    dn_ok = dn is not None and dn[0] < 0 and _touches(dn, highs, tol) >= min_touches
    n = len(w) - 1                # current bar index
    if up_ok and (not dn_ok or _touches(up, lows, tol) >= _touches(dn, highs, tol)):
        lower = up[0] * n + up[1]
        upper = dn[0] * n + dn[1] if dn is not None else None
        return "up", lower, upper
    if dn_ok:
        upper = dn[0] * n + dn[1]
        lower = up[0] * n + up[1] if up is not None else None
        return "down", lower, upper
    return "range", None, None


def rail_entry(bars: Sequence[Bar], k: int = 2, lookback: int = 60,
               tol: float = 30.0, min_touches: int = 3,
               rail_tol: float = 15.0,
               triggers: tuple[str, ...] = ("engulf",),
               hold: bool = False) -> Optional[str]:
    """§3.4 順勢在軌道進場: 上升軌回踩下軌 → 多; 下降軌回彈上軌 → 空. 位置 (rail)
    是 edge — 只在 rail_tol 內、且任一 `triggers` 扣板機同向才進 (收K).
    hold=True 額外要求收盤守在 rail 正確側 (多:close≥rail 支撐守住; 空:close≤rail)."""
    direction, lower, upper = channel(bars, k, lookback, tol, min_touches)
    if direction == "up" and lower is not None:
        want, rail = "long", lower
    elif direction == "down" and upper is not None:
        want, rail = "short", upper
    else:
        return None
    close = bars[-1].close
    if abs(close - rail) > rail_tol:
        return None
    if hold and ((want == "long" and close < rail) or (want == "short" and close > rail)):
        return None
    for t in triggers:
        if _RAIL_TRIGGERS[t](bars) == want:
            return want
    return None


# ── §3.6 送錢盤法 (開盤短力道) ────────────────────────────────────────────

def _prev_day_hilo(bars: Sequence[Bar]) -> tuple[Optional[float], Optional[float]]:
    """High/low of the most recent completed DAY session before the last bar's date."""
    last_date = bars[-1].ts.date()
    hi = lo = None
    target = None
    for b in reversed(bars[:-1]):
        if clock.session_of(b.ts) is not clock.Session.DAY or b.ts.date() == last_date:
            continue
        if target is None:
            target = b.ts.date()          # lock onto the most recent prior day session
        if b.ts.date() != target:
            break
        hi = b.high if hi is None else max(hi, b.high)
        lo = b.low if lo is None else min(lo, b.low)
    return hi, lo


def _day_session_bars(bars: Sequence[Bar]) -> list[Bar]:
    """Trailing bars belonging to the last bar's DAY session (empty if not DAY)."""
    last = bars[-1]
    if clock.session_of(last.ts) is not clock.Session.DAY:
        return []
    out: list[Bar] = []
    for b in reversed(bars):
        if clock.session_of(b.ts) is clock.Session.DAY and b.ts.date() == last.ts.date():
            out.append(b)
        else:
            break
    out.reverse()
    return out


def gift_play(bars: Sequence[Bar], k: int = 2, lookback: int = 40,
              tol: float = 30.0, min_touches: int = 2,
              open_bars: int = 3, retest_tol: float = 10.0) -> Optional[str]:
    """§3.6 送錢盤法: 開盤跳空站上昨日高/低撐壓 (session-open gap) + 開盤在軌道半山
    以上 + 前 open_bars 根內回測該撐壓後守住 → 順軌道方向. The retest need not be the
    first bar (rare intrabar); scan the opening bars while the level holds."""
    sb = _day_session_bars(bars)
    if not sb or len(sb) > open_bars:               # only in the opening window
        return None
    hi, lo = _prev_day_hilo(bars)
    if hi is None or lo is None:
        return None
    open0 = sb[0].open                              # session-open gap reference
    direction, lower, upper = channel(bars, k, lookback, tol, min_touches)
    mid = (lower + upper) / 2.0 if (lower is not None and upper is not None) else None
    b = bars[-1]
    if open0 > hi and direction == "up" and (mid is None or open0 >= mid):
        if b.low <= hi + retest_tol and b.close > hi:   # retest prev-day high, hold
            return "long"
    if open0 < lo and direction == "down" and (mid is None or open0 <= mid):
        if b.high >= lo - retest_tol and b.close < lo:  # retest prev-day low, hold
            return "short"
    return None
