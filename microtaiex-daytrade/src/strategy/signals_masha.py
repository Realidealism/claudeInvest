"""麻紗 day-trade entry signals (MASHA_FUTURES_SPEC §4-§6, M1 subset).

Pure price-action over closed 5m bars (oldest..newest, last = current). All are
★/◐ automatable per §10. Directional helpers return "long" / "short" / None;
gate helpers return bool. Tolerances are parameters (swept later in backtest).

  engulf / engulf_strength   §4.2  吞噬 (收K, 含引線加分)
  redblack_zone / _lazy      §5.5  紅黑交錯懶人法 (框住前一交錯區，被吞噬定方向)
  four_hand                  §6-3  四手 (四根等長紅黑交錯短引線 → 等方向)
  doji_flip                  §6-6  十字變盤 (吞過十字引線 = 完成 SOP)
  island_reversal            §6-8  島狀反轉 (K 被跳空孤立 → 強反轉)
"""
from __future__ import annotations

from typing import Optional, Sequence

from broker.types import Bar


def _top(b: Bar) -> float:
    return max(b.open, b.close)


def _bot(b: Bar) -> float:
    return min(b.open, b.close)


def _is_red(b: Bar) -> bool:      # 紅K (漲): close > open
    return b.close > b.open


def _is_black(b: Bar) -> bool:    # 黑K (跌): close < open
    return b.close < b.open


# ── §4.2 吞噬 ────────────────────────────────────────────────────────────

def engulf(bars: Sequence[Bar]) -> Optional[str]:
    """Bullish/bearish engulfing on the current bar (body cover, 收K判定).
    Returns "long" (紅吞黑) / "short" (黑吞紅) / None."""
    if len(bars) < 2:
        return None
    prev, curr = bars[-2], bars[-1]
    if _is_black(prev) and _is_red(curr) and curr.open <= prev.close and curr.close >= prev.open:
        return "long"
    if _is_red(prev) and _is_black(curr) and curr.open >= prev.close and curr.close <= prev.open:
        return "short"
    return None


def engulf_strength(bars: Sequence[Bar]) -> float:
    """1.0 if the engulf also covers the previous bar's wicks (含引線), 0.8 if
    body-only (未吞引線約降 20%, §4.2), 0.0 if no engulf."""
    side = engulf(bars)
    if side is None:
        return 0.0
    prev, curr = bars[-2], bars[-1]
    return 1.0 if (curr.high >= prev.high and curr.low <= prev.low) else 0.8


# ── §5.5 紅黑交錯懶人法 ──────────────────────────────────────────────────

def redblack_zone(bars: Sequence[Bar], zone_lb: int = 8,
                  tol: float = 0.0) -> Optional[tuple[float, float]]:
    """(body_hi, body_lo) of the most recent 紅黑交錯 cluster ending at the last
    bar: consecutive bars whose bodies overlap AND that contain both colours.
    None if no such zone."""
    if len(bars) < 2:
        return None
    window = bars[-zone_lb:]
    hi, lo = _top(window[-1]), _bot(window[-1])
    members = [window[-1]]
    for b in reversed(window[:-1]):
        bt, bb = _top(b), _bot(b)
        if bb <= hi + tol and bt >= lo - tol:      # overlaps the running zone
            hi, lo = max(hi, bt), min(lo, bb)
            members.append(b)
        else:
            break
    if len(members) < 2:
        return None
    reds = any(_is_red(b) for b in members)
    blacks = any(_is_black(b) for b in members)
    if not (reds and blacks):                       # need 紅黑 both present
        return None
    return hi, lo


def redblack_lazy(bars: Sequence[Bar], zone_lb: int = 8,
                  tol: float = 0.0) -> Optional[str]:
    """框住『當下位置的前一個』紅黑交錯區，框高被當下K吞噬→做多、框低→做空 (收K)."""
    if len(bars) < 3:
        return None
    zone = redblack_zone(bars[:-1], zone_lb, tol)
    if zone is None:
        return None
    hi, lo = zone
    curr = bars[-1]
    if curr.close > hi:
        return "long"
    if curr.close < lo:
        return "short"
    return None


# ── §6 型態 (gate / flag) ────────────────────────────────────────────────

def four_hand(bars: Sequence[Bar], tol: float = 0.35) -> bool:
    """四手 (§6-3): last 4 bars similar-length, 紅黑交錯, short wicks (consolidation
    → wait for the breakout to trade). Returns True if the pattern is present."""
    if len(bars) < 4:
        return False
    last4 = bars[-4:]
    bodies = [abs(b.close - b.open) for b in last4]
    avg = sum(bodies) / 4
    if avg <= 0 or any(abs(bd - avg) > tol * avg for bd in bodies):
        return False
    reds = sum(1 for b in last4 if _is_red(b))
    if reds in (0, 4):                              # need both colours (交錯)
        return False
    for b in last4:                                 # short wicks: body dominates range
        rng = b.high - b.low
        if rng > 0 and abs(b.close - b.open) / rng < 0.5:
            return False
    return True


def doji_flip(bars: Sequence[Bar], doji_ratio: float = 0.1) -> Optional[str]:
    """十字變盤 (§6-6): prev = doji (tiny body), current closes engulfing past the
    doji's wick → 完成 SOP, direction by current colour."""
    if len(bars) < 2:
        return None
    prev, curr = bars[-2], bars[-1]
    rng = prev.high - prev.low
    if rng <= 0 or abs(prev.close - prev.open) / rng > doji_ratio:
        return None                                 # prev not a doji
    if curr.close > prev.high and _is_red(curr):
        return "long"
    if curr.close < prev.low and _is_black(curr):
        return "short"
    return None


def exhaustion(bars: Sequence[Bar], side: str) -> bool:
    """力竭 (§1, §2.4c): the move stalls and reverses against `side` — a reverse
    engulf, a doji (力道用盡), or a long rejection wick. Used as an exit trigger."""
    if len(bars) < 2:
        return False
    curr = bars[-1]
    e = engulf(bars)
    if (side == "long" and e == "short") or (side == "short" and e == "long"):
        return True
    rng = curr.high - curr.low
    if rng <= 0:
        return False
    if abs(curr.close - curr.open) / rng < 0.1:          # 十字
        return True
    if side == "long" and (curr.high - _top(curr)) / rng > 0.6:   # 上引線 rejection
        return True
    if side == "short" and (_bot(curr) - curr.low) / rng > 0.6:   # 下引線 rejection
        return True
    return False


def island_reversal(bars: Sequence[Bar], min_gap: float = 0.0) -> Optional[str]:
    """島狀反轉 (§6-8): the middle bar is gapped away from the bar before AND the
    bar after, in opposite directions → strong reversal."""
    if len(bars) < 3:
        return None
    a, isl, c = bars[-3], bars[-2], bars[-1]
    if isl.low > a.high + min_gap and c.high < isl.low - min_gap:
        return "short"                              # top island → reverse down
    if isl.high < a.low - min_gap and c.low > isl.high + min_gap:
        return "long"                               # bottom island → reverse up
    return None
