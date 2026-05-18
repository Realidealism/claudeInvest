"""
Short-period overheat scoring — independent of ScoreBoard.

Designed to fill the gap left by ScoreBoard v150's long-period bias (扣抵_short
removed, 扣抵_long upgraded ±5→±15, 波浪_medium removed). flee signals previously
relied on `_short_pct_array` for short-term reversal detection, but ScoreBoard
has drifted toward "slow trend confirmation" semantics — score急升 now mostly
means "long-period MA finally turned" rather than "stock just overheated".

OverheatBoard provides a parallel scoring channel focused exclusively on
short-period (3/5/8 MA, 1-3 day windows) signals. Output normalized to
-100~+100 same as ScoreBoard.pct so flee can swap thresholds 1:1.

NO timeframes — single board, single side-pair. Knot rescue NOT applied
(overheat semantics ≠ contrarian rescue).

Usage:
    from analysis.overheat_board import build_overheat_board

    board = build_overheat_board()
    result = board.evaluate(data, day_index)
    print(result.long.pct)   # -100 ~ +100
    print(result.short.pct)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

from analysis.score import (
    ScoreItem,
    ScoreDetail,
    SideScore,
    TimeframeScore,
    bool_score,
    _eval_side,
    _gold_within,
    _death_within,
    _obv_up_within,
    _obv_down_within,
    _wave_event_within,
    _MACD_FRESH_WINDOW,
    _MACD_CARRY_WINDOW,
)

if TYPE_CHECKING:
    from backtest.data import StockData


# ── OverheatBoard ──────────────────────────────────────────────────────────


class OverheatBoard:
    """Single-board short-period overheat scoring.

    Unlike ScoreBoard, no short/medium/long card split. All cells operate
    on short-period (3/5/8 MA, 1-8 day) indicators.
    """

    def __init__(self, name: str = "短期過熱"):
        self.name = name
        self.long_items: list[ScoreItem] = []
        self.short_items: list[ScoreItem] = []

    def add_long(self, item: ScoreItem) -> None:
        self.long_items.append(item)

    def add_short(self, item: ScoreItem) -> None:
        self.short_items.append(item)

    def evaluate(self, data: "StockData", i: int) -> TimeframeScore:
        long = _eval_side(self.long_items, data, i)
        short = _eval_side(self.short_items, data, i)
        return TimeframeScore(long=long, short=short)


# ── Continuous cell helper: 1-day return / 8-day ATR-like ──────────────────


def _intraday_ret_z_array(data: "StockData") -> NDArray[np.float32]:
    """Per-bar signed (today_return / 8d_lagged_abs_return_mean), clipped to
    ±2, scaled to ±5 points. Cached on `data` instance.

    Window is lagged (excludes today) to avoid look-ahead in the normalizer.
    """
    cache = getattr(data, "_intraday_ret_z_cache", None)
    if cache is not None:
        return cache
    close = np.asarray(data.close, dtype=np.float32)
    n = len(close)
    out = np.zeros(n, dtype=np.float32)
    if n < 10:
        data._intraday_ret_z_cache = out
        return out
    rets = np.zeros(n, dtype=np.float32)
    rets[1:] = (close[1:] - close[:-1]) / np.maximum(close[:-1], 1e-6)
    abs_rets = np.abs(rets)
    win = 8
    rolling = np.zeros(n, dtype=np.float32)
    for i in range(win + 1, n):
        rolling[i] = abs_rets[i - win:i].mean()
    rolling_safe = np.maximum(rolling, 1e-4)
    z = rets / rolling_safe
    z_clip = np.clip(z, -2.0, 2.0)
    pts = (z_clip / 2.0) * 5.0
    data._intraday_ret_z_cache = pts.astype(np.float32)
    return data._intraday_ret_z_cache


def _ret_z_long(d: "StockData", i: int) -> float:
    """Positive when stock rose today relative to its 8-day volatility."""
    return float(_intraday_ret_z_array(d)[i])


def _ret_z_short(d: "StockData", i: int) -> float:
    """Mirror — positive when stock fell today relative to its 8-day volatility."""
    return -float(_intraday_ret_z_array(d)[i])


# ── Factory ────────────────────────────────────────────────────────────────


def build_overheat_board() -> OverheatBoard:
    """Build OverheatBoard — short-period reversal-friendly score channel.

    v8 — LOO ablation 2026-05-18 顯示 OBV 是最大 noise (拿掉 fwd5 +0.071 最大改善).
    v7 minimal (砍 MACD/OBV/Wave 全部) 結果 portfolio PF -0.148 失敗 (LOO 互動效應失效).
    v8 折衷：只砍 OBV，保留 MACD/Wave 維持 cell 多樣性。

    Cells (~±25 raw points per side, 15 cells per side):
      - 5MA 漲扣 / 跌扣 ±3
      - 8MA 漲扣 / 跌扣 ±3
      - 1日 return / 8日 ATR (continuous) ±5
      - MACD short 金叉_新/續 死叉_新/續 ±3/±2
      - 浪D4 (short scope wave 2/4 MA cross) 金叉_新/續 死叉_新/續 ±3/±2
      - 短洪量 站上/跌破 1階洪 ±3
      (v7 dropped: OBV short events ±3/±2)

    Cells deliberately exclude trend-confirmation extensions tested and
    rejected 2026-05-16:
      - 純蠟燭 (red_short / black_short / jump / squat): no IC contribution,
        hurt sell_main fwd5 predictive power
      - 量帶蠟燭 (AND'd vx_burst): worse — pure trend-follower signal,
        violates flee's contrarian semantics
      - OverBreakout (人/走/召 × 漲/跌): +0.022 IC but breaks
        reversal-friendly filter — sell_OH_l>=75 fwd5 dropped from
        +0.992% (17c) to +0.314%

    This 17c version is the "reversal-friendly sweet spot" for flee
    post_strength gate use.
    """
    board = OverheatBoard("短期過熱")

    # ── 5MA / 8MA turn cells ──
    for period, pts in ((5, 3.0), (8, 3.0)):
        # 漲扣: long +pts, short -pts
        board.add_long(bool_score(
            f"{period}MA漲扣", pts,
            lambda d, i, p=period: d.close_result.turn[p][i] == 2, "扣抵",
        ))
        board.add_short(bool_score(
            f"{period}MA漲扣", -pts,
            lambda d, i, p=period: d.close_result.turn[p][i] == 2, "扣抵",
        ))
        # 跌扣: long -pts, short +pts
        board.add_long(bool_score(
            f"{period}MA跌扣", -pts,
            lambda d, i, p=period: d.close_result.turn[p][i] == 0, "扣抵",
        ))
        board.add_short(bool_score(
            f"{period}MA跌扣", pts,
            lambda d, i, p=period: d.close_result.turn[p][i] == 0, "扣抵",
        ))

    # ── 1日 return / ATR continuous ±5 ──
    board.add_long(ScoreItem(
        name="日內漲跌_z", points=5.0,
        evaluate=_ret_z_long, category="衝擊", continuous=True,
    ))
    board.add_short(ScoreItem(
        name="日內漲跌_z", points=5.0,
        evaluate=_ret_z_short, category="衝擊", continuous=True,
    ))

    # ── MACD short event-window cells ──
    _add_macd_short_cells(board)

    # ── Wave d4 (short scope) event-window cells ──
    _add_wave_d4_cells(board)

    # ── Flood (short scope, tier 1) cells (v6) ──
    _add_flood_short_cells(board)

    # v8 dropped: OBV short events (LOO showed -OBV fwd5 +0.071 — biggest noise).
    # _add_obv_short_cells kept as dormant helper for future re-introduction.

    return board


def _add_flood_short_cells(board: OverheatBoard) -> None:
    """Short-scope flood reference (tier 1): 站上/跌破 最近一次洪量參考價.
    Brings volume-based reversal confirmation into OH. ±3 same scale as 扣抵.
    """
    pts = 3.0
    tier = 1
    board.add_long(bool_score(
        "站上1階洪", pts,
        lambda d, i: bool(d.volume_result.above_tier[tier][i]), "洪量",
    ))
    board.add_short(bool_score(
        "站上1階洪", -pts,
        lambda d, i: bool(d.volume_result.above_tier[tier][i]), "洪量",
    ))
    board.add_long(bool_score(
        "跌破1階洪", -pts,
        lambda d, i: bool(d.volume_result.below_tier[tier][i]), "洪量",
    ))
    board.add_short(bool_score(
        "跌破1階洪", pts,
        lambda d, i: bool(d.volume_result.below_tier[tier][i]), "洪量",
    ))


def _add_macd_short_cells(board: OverheatBoard) -> None:
    """MACD short scope gold/death two-tier events. Smaller pts (±3/±2)
    than SB short MACD (±5/±3) — OH is a minimal short-term overheat board,
    individual cells shouldn't dominate.
    """
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    p_fresh, p_carry = 3.0, 2.0
    scope = "short"

    def gold_fresh(d, i):
        return _gold_within(d, i, scope, fresh)

    def gold_carry(d, i):
        return _gold_within(d, i, scope, carry) and not _gold_within(d, i, scope, fresh)

    def death_fresh(d, i):
        return _death_within(d, i, scope, fresh)

    def death_carry(d, i):
        return _death_within(d, i, scope, carry) and not _death_within(d, i, scope, fresh)

    board.add_long(bool_score("短MACD金叉_新", p_fresh, gold_fresh, "MACD"))
    board.add_long(bool_score("短MACD金叉_續", p_carry, gold_carry, "MACD"))
    board.add_long(bool_score("短MACD死叉_新", -p_fresh, death_fresh, "MACD"))
    board.add_long(bool_score("短MACD死叉_續", -p_carry, death_carry, "MACD"))

    board.add_short(bool_score("短MACD金叉_新", -p_fresh, gold_fresh, "MACD"))
    board.add_short(bool_score("短MACD金叉_續", -p_carry, gold_carry, "MACD"))
    board.add_short(bool_score("短MACD死叉_新", p_fresh, death_fresh, "MACD"))
    board.add_short(bool_score("短MACD死叉_續", p_carry, death_carry, "MACD"))


def _add_obv_short_cells(board: OverheatBoard) -> None:
    """OBV short scope signal_up/down two-tier events. Same ±3/±2 sizing."""
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    p_fresh, p_carry = 3.0, 2.0
    scope = "short"

    def up_fresh(d, i):
        return _obv_up_within(d, i, scope, fresh)

    def up_carry(d, i):
        return _obv_up_within(d, i, scope, carry) and not _obv_up_within(d, i, scope, fresh)

    def down_fresh(d, i):
        return _obv_down_within(d, i, scope, fresh)

    def down_carry(d, i):
        return _obv_down_within(d, i, scope, carry) and not _obv_down_within(d, i, scope, fresh)

    board.add_long(bool_score("短OBV升_新", p_fresh, up_fresh, "OBV"))
    board.add_long(bool_score("短OBV升_續", p_carry, up_carry, "OBV"))
    board.add_long(bool_score("短OBV降_新", -p_fresh, down_fresh, "OBV"))
    board.add_long(bool_score("短OBV降_續", -p_carry, down_carry, "OBV"))

    board.add_short(bool_score("短OBV升_新", -p_fresh, up_fresh, "OBV"))
    board.add_short(bool_score("短OBV升_續", -p_carry, up_carry, "OBV"))
    board.add_short(bool_score("短OBV降_新", p_fresh, down_fresh, "OBV"))
    board.add_short(bool_score("短OBV降_續", p_carry, down_carry, "OBV"))


def _add_wave_d4_cells(board: OverheatBoard) -> None:
    """Wave d4 (short scope: 2 MA cross 4 MA on wave tips) two-tier events.
    Same ±3/±2 sizing as MACD/OBV. Mirror SB short scope wave but in OH context.
    """
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    p_fresh, p_carry = 3.0, 2.0
    gold_attr, death_attr = "wave_d4_gold", "wave_d4_death"

    def gold_fresh(d, i):
        return _wave_event_within(d, i, gold_attr, fresh)

    def gold_carry(d, i):
        return _wave_event_within(d, i, gold_attr, carry) and not _wave_event_within(d, i, gold_attr, fresh)

    def death_fresh(d, i):
        return _wave_event_within(d, i, death_attr, fresh)

    def death_carry(d, i):
        return _wave_event_within(d, i, death_attr, carry) and not _wave_event_within(d, i, death_attr, fresh)

    board.add_long(bool_score("短浪D4金_新", p_fresh, gold_fresh, "波浪"))
    board.add_long(bool_score("短浪D4金_續", p_carry, gold_carry, "波浪"))
    board.add_long(bool_score("短浪D4死_新", -p_fresh, death_fresh, "波浪"))
    board.add_long(bool_score("短浪D4死_續", -p_carry, death_carry, "波浪"))

    board.add_short(bool_score("短浪D4金_新", -p_fresh, gold_fresh, "波浪"))
    board.add_short(bool_score("短浪D4金_續", -p_carry, gold_carry, "波浪"))
    board.add_short(bool_score("短浪D4死_新", p_fresh, death_fresh, "波浪"))
    board.add_short(bool_score("短浪D4死_續", p_carry, death_carry, "波浪"))


# ── Fingerprint for cache invalidation ─────────────────────────────────────


OVERHEAT_CACHE_VERSION = "v8"  # v8: drop OBV only (LOO biggest noise), keep MACD/Wave (15c)


def overheat_fingerprint(board: OverheatBoard) -> str:
    """12-char hash of OverheatBoard cell config. Mirrors board_fingerprint().

    Bump OVERHEAT_CACHE_VERSION when a cell's evaluate lambda logic changes
    without its name/points/category changing.
    """
    import hashlib
    parts: list = [OVERHEAT_CACHE_VERSION]
    for side_tag, items in (("L", board.long_items), ("S", board.short_items)):
        for it in items:
            parts.append((side_tag, it.name, float(it.points),
                          it.category, it.continuous))
    return hashlib.md5(repr(parts).encode()).hexdigest()[:12]
