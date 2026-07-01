"""麻紗 框形價位計算 (MASHA_FUTURES_SPEC §7, M2 subset).

Measured-move / 幅位 helpers for the 滿足點 (satisfaction target, §2.4a). Used
ONLY for exits/targets — never as an entry reason (§7 通則). M2 ships the core
1:1 力道對稱 (§7.1); gap-box / N-字 / 跳空衝出 / 假破對稱 / §7.6 歸還框 are M2b.
"""
from __future__ import annotations

from typing import Optional, Sequence

from broker.types import Bar


def impulse_box(bars: Sequence[Bar], lookback: int = 10) -> Optional[tuple[float, float, str]]:
    """(lo, hi, direction) of the dominant impulse (力道框) over the last N bars.
    direction "up" if the swing high forms after the swing low, else "down".
    None if the window is flat or too short."""
    if len(bars) < 2:
        return None
    w = bars[-lookback:]
    lo = min(b.low for b in w)
    hi = max(b.high for b in w)
    if hi <= lo:
        return None
    i_lo = min(range(len(w)), key=lambda i: w[i].low)
    i_hi = max(range(len(w)), key=lambda i: w[i].high)
    return lo, hi, ("up" if i_hi > i_lo else "down")


def box_height(box: tuple[float, float, str]) -> float:
    return box[1] - box[0]


def midline(box: tuple[float, float, str]) -> float:
    """慈母線 — the 1:1 box mid-line (defense / re-entry level, §7.1)."""
    return (box[0] + box[1]) / 2.0


def target_1to1(side: str, ref_price: float,
                box: tuple[float, float, str]) -> float:
    """1:1 力道對稱 滿足點 (§7.1): project one box-height beyond the entry."""
    h = box_height(box)
    return ref_price + h if side == "long" else ref_price - h
