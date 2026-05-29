"""
Technical analysis scoring system.

Three timeframe score cards (short/medium/long) with separate long/short
scores, plus a combined total.

Timeframe MA periods:
  short:  3, 5, 8
  medium: 21, 34, 55
  long:   144, 233, 377

Usage:
    from analysis.score import ScoreBoard, ScoreCard, bool_score

    board = ScoreBoard()
    board.short.add_long(bool_score("OBV買訊", 2, ...))
    board.medium.add_short(bool_score("短排空", 2, ...))

    result = board.evaluate(data, day_index)
    print(result.short)           # SideScore for short timeframe
    print(result.total.long_score)  # combined long score
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from backtest.data import StockData


# ── Timeframe period groups ────────────────────────────────────────────────

SHORT_PERIODS = (3, 5, 8)
MEDIUM_PERIODS = (21, 34, 55)
LONG_PERIODS = (144, 233, 377)


# ── Weight multipliers: C dampened (2026-05-04, train-derived but moderated)
# Step 1: A1 raw multipliers from _score_train_test_weights.py — train period
#         2017-2020, m_raw = clip(1 + 5×Δ, 0.3, 2.0) per cell.
# Step 2: dampened to half-distance from 1.0: m_final = (m_raw + 1) / 2.
#
# A1 raw production result: H=60 full +0.113pp ✓ but H=60 bear -0.143pp ✗
# (train was bull-only so bear regime wasn't part of fit). C dampened
# synthesized: H=60 full +0.083pp / H=60 bear -0.078pp — trades ~40% of
# alpha for ~30% bear regime relief.
WEIGHT_MULTIPLIERS = {
    ("扣抵", "medium"): 1.185,    # raw 1.370
    ("扣抵", "long"): 1.099,      # raw 1.198
    ("排列", "medium"): 0.969,    # raw 0.937
    ("排列", "long"): 1.341,      # raw 1.681
    ("大盤", "long"): 1.096,      # raw 1.191
    ("洪量", "short"): 0.650,     # raw 0.300
    ("洪量", "medium"): 0.924,    # raw 0.848
    ("洪量", "long"): 1.041,      # raw 1.081
    ("MACD", "short"): 0.878,     # raw 0.756
    ("MACD", "medium"): 1.013,    # raw 1.026
    ("OBV", "short"): 0.815,      # raw 0.629
    ("OBV", "medium"): 1.033,     # raw 1.066
    ("OBV", "long"): 0.938,       # raw 0.876
    ("波浪", "short"): 0.912,     # raw 0.824
    ("波浪", "medium"): 1.036,    # raw 1.071
    ("波浪", "long"): 1.131,      # raw 1.262
    # ("Donchian", "long"): 0.980 was for Lucas 123; testing Fib 144 now,
    # multiplier reset to 1.0 for clean ablation
}


def _w(cat: str, scope: str) -> float:
    """Look up train-derived weight multiplier; defaults to 1.0 if not set."""
    return WEIGHT_MULTIPLIERS.get((cat, scope), 1.0)


# ── Dataclasses ────────────────────────────────────────────────────────────


@dataclass
class ScoreItem:
    """One scored condition."""
    name: str
    points: float
    evaluate: Callable[[StockData, int], bool]
    category: str = ""
    continuous: bool = False  # If True, evaluate returns float ∈ [-points, +points]
                               # used directly as pts (not bool-thresholded)


@dataclass
class ScoreDetail:
    """Result of a single condition evaluation."""
    name: str
    triggered: bool
    points: float
    category: str


@dataclass
class SideScore:
    """Score result for one side (long or short)."""
    score: float
    max_possible: float     # best case (all positive items triggered)
    min_possible: float     # worst case (all negative items triggered)
    details: list[ScoreDetail]

    @property
    def pct(self) -> float:
        """Score as percentage of full range, mapped to -100 ~ +100."""
        span = self.max_possible - self.min_possible
        if span == 0:
            return 0.0
        return (self.score - self.min_possible) / span * 200 - 100

    def triggered(self) -> list[ScoreDetail]:
        return [d for d in self.details if d.triggered]

    def by_category(self) -> dict[str, float]:
        cats: dict[str, float] = {}
        for d in self.details:
            if d.triggered:
                cats[d.category] = cats.get(d.category, 0) + d.points
        return cats


@dataclass
class TimeframeScore:
    """Score result for one timeframe (long side + short side)."""
    long: SideScore
    short: SideScore

    @property
    def long_score(self) -> float:
        return self.long.score

    @property
    def short_score(self) -> float:
        return self.short.score


@dataclass
class BoardResult:
    """Complete scoring result across all timeframes."""
    short: TimeframeScore
    medium: TimeframeScore
    long: TimeframeScore
    total: TimeframeScore     # combined across all timeframes


# ── Knot rescue (mirror Go BuyTrendLeverageValue line 7259-7419) ──────────


@dataclass(frozen=True)
class KnotRescueConfig:
    """Pull extreme total-side scores toward the middle when SMA bands knot.

    Trigger ratios are vs |min_possible| / |max_possible| of the SideScore.
    Each stage adds (or subtracts at the cap) `rescue_points` only if the
    current score still exceeds the threshold — rescues compound across
    stages because the score updates between checks.
    """
    short_ratio: float = 0.65
    medium_ratio: float = 0.60
    long_ratio: float = 0.55
    dual_ratio: float = 0.50      # long + medium knot simultaneously
    triple_ratio: float = 0.45    # short + medium + long knot simultaneously
    rescue_points: float = 5.0
    cap_rescue: bool = True       # 5-day knot also pulls the top tail down


# ── ScoreCard (one timeframe) ──────────────────────────────────────────────


class ScoreCard:
    """Score card for a single timeframe with separate long/short conditions."""

    def __init__(self, name: str):
        self.name = name
        self.long_items: list[ScoreItem] = []
        self.short_items: list[ScoreItem] = []

    def add_long(self, item: ScoreItem) -> None:
        self.long_items.append(item)

    def add_short(self, item: ScoreItem) -> None:
        self.short_items.append(item)

    def evaluate(self, data: StockData, i: int) -> TimeframeScore:
        long = _eval_side(self.long_items, data, i)
        short = _eval_side(self.short_items, data, i)
        return TimeframeScore(long=long, short=short)


# ── ScoreBoard (all timeframes) ───────────────────────────────────────────


class ScoreBoard:
    """
    Three-timeframe scoring board.

    board.short  — short timeframe card  (3, 5, 8)
    board.medium — medium timeframe card (21, 34, 55)
    board.long   — long timeframe card   (144, 233, 377)

    evaluate() returns per-timeframe scores plus a combined total.
    """

    def __init__(
        self,
        name: str = "技術評分",
        knot_config: KnotRescueConfig | None = None,
    ):
        self.name = name
        self.short = ScoreCard("短週期")
        self.medium = ScoreCard("中週期")
        self.long = ScoreCard("長週期")
        # Default-on: pass knot_config=None explicitly to disable rescue
        self.knot_config = knot_config if knot_config is not None else KnotRescueConfig()

    def evaluate(self, data: StockData, i: int) -> BoardResult:
        s = self.short.evaluate(data, i)
        m = self.medium.evaluate(data, i)
        l = self.long.evaluate(data, i)

        # Combined total
        total_long = SideScore(
            score=s.long.score + m.long.score + l.long.score,
            max_possible=s.long.max_possible + m.long.max_possible + l.long.max_possible,
            min_possible=s.long.min_possible + m.long.min_possible + l.long.min_possible,
            details=s.long.details + m.long.details + l.long.details,
        )
        total_short = SideScore(
            score=s.short.score + m.short.score + l.short.score,
            max_possible=s.short.max_possible + m.short.max_possible + l.short.max_possible,
            min_possible=s.short.min_possible + m.short.min_possible + l.short.min_possible,
            details=s.short.details + m.short.details + l.short.details,
        )

        if self.knot_config is not None:
            _apply_knot_rescue(total_long, data, i, self.knot_config)
            _apply_knot_rescue(total_short, data, i, self.knot_config)

        return BoardResult(
            short=s,
            medium=m,
            long=l,
            total=TimeframeScore(long=total_long, short=total_short),
        )


# ── Internal ───────────────────────────────────────────────────────────────


def _eval_side(
    items: list[ScoreItem], data: StockData, i: int,
) -> SideScore:
    details = []
    score = 0.0
    max_possible = 0.0
    min_possible = 0.0

    for item in items:
        if item.continuous:
            # Continuous: evaluate returns signed float ∈ [-points, +points];
            # both ends contribute to max/min_possible.
            cap = abs(item.points)
            max_possible += cap
            min_possible -= cap
            val = float(item.evaluate(data, i))
            pts = max(-cap, min(cap, val))
            triggered = pts != 0.0
        else:
            if item.points > 0:
                max_possible += item.points
            else:
                min_possible += item.points
            triggered = bool(item.evaluate(data, i))
            pts = item.points if triggered else 0.0
        score += pts
        details.append(ScoreDetail(
            name=item.name,
            triggered=triggered,
            points=pts,
            category=item.category,
        ))

    return SideScore(
        score=score, max_possible=max_possible,
        min_possible=min_possible, details=details,
    )


def _apply_knot_rescue(
    side: SideScore,
    data: StockData,
    i: int,
    cfg: KnotRescueConfig,
) -> None:
    """Compound knot-rescue stages; mutates side.score in place.

    Mirrors Go CalculateTrade2.go:7259-7419. Each scope (short/medium/long)
    runs a 1-/3-/5-day streak ladder; long+medium and triple-knot add two
    extra floor stages. The 5-day stage also caps the top tail when
    cfg.cap_rescue is True (Go line 7302-7308).
    """
    knot = data.close_result.knot
    short_k = bool(knot["short"].flag[i])
    medium_k = bool(knot["medium"].flag[i])
    long_k = bool(knot["long"].flag[i])
    if not (short_k or medium_k or long_k):
        return

    pts = cfg.rescue_points
    abs_min = abs(side.min_possible) or 1.0
    abs_max = abs(side.max_possible) or 1.0
    score = side.score

    def _streak(flag, ratio: float) -> float:
        s = score
        if not bool(flag[i]):
            return s
        if s < -ratio * abs_min:
            s += pts
        if i >= 2 and bool(flag[i - 1]) and bool(flag[i - 2]):
            if s < -ratio * abs_min:
                s += pts
            if i >= 4 and bool(flag[i - 3]) and bool(flag[i - 4]):
                if s < -ratio * abs_min:
                    s += pts
                if cfg.cap_rescue and s > ratio * abs_max:
                    s -= pts
        return s

    score = _streak(knot["short"].flag, cfg.short_ratio)
    score = _streak(knot["medium"].flag, cfg.medium_ratio)
    score = _streak(knot["long"].flag, cfg.long_ratio)

    if long_k and medium_k and score < -cfg.dual_ratio * abs_min:
        score += pts
    if short_k and medium_k and long_k and score < -cfg.triple_ratio * abs_min:
        score += pts

    side.score = score


# ── Convenience factories ──────────────────────────────────────────────────


def bool_score(
    name: str,
    points: float,
    accessor: Callable[[StockData, int], bool],
    category: str = "",
) -> ScoreItem:
    """Score from a boolean condition."""
    return ScoreItem(name=name, points=points, evaluate=accessor, category=category)


def build_turn_scoreboard() -> ScoreBoard:
    """
    Build a ScoreBoard with turn-point (扣抵) scoring rules.

    Short (3,5,8):   each ±5, fuzzy 13 → ±5 when all neutral
    Medium (21,34,55): each ±5, fuzzy 13 ±2.5 & 89 ±2.5 when all neutral
    Long (144,233,377): each ±5, fuzzy 89 → ±5 when all neutral
    """
    board = ScoreBoard("扣抵評分")
    _add_turn_rules(board)
    return board


def board_fingerprint(board: ScoreBoard) -> str:
    """12-char hash of board's cell config (name, points, category, continuous)
    plus knot rescue config. Used as cache key so pct arrays auto-invalidate
    when ScoreBoard cells/weights/knot config change.

    Bump SCORE_CACHE_VERSION below to manually invalidate (e.g. when a cell's
    evaluate lambda logic changes without its name/points/category changing)."""
    import hashlib
    parts: list = [SCORE_CACHE_VERSION]
    for card_tag, card in (
        ("s", board.short), ("m", board.medium), ("l", board.long),
    ):
        for side_tag, items in (("L", card.long_items), ("S", card.short_items)):
            for it in items:
                parts.append((card_tag, side_tag, it.name, float(it.points),
                              it.category, it.continuous))
    if board.knot_config is not None:
        kc = board.knot_config
        parts.append(("knot", repr(sorted(vars(kc).items()))))
    return hashlib.md5(repr(parts).encode()).hexdigest()[:12]


# Bump when a cell's evaluate lambda logic changes without its name/points
# changing (e.g. tweaking thresholds inside the lambda body). Most config
# changes are caught automatically by board_fingerprint().
SCORE_CACHE_VERSION = "v3"


def build_scoreboard() -> ScoreBoard:
    """
    Build a unified ScoreBoard with all technical scoring rules.

    Turn (扣抵):     per-MA ±5, fuzzy conditions when neutral (medium + long)
    Sort (排列):     sort_normal ±10 (medium + long)
    Forming (成形):  sort_forming ±5 (medium + long)
    Breadth (大盤):  market trend vs stock sort alignment (medium + long)
    Flood (洪量):    ±15 per timeframe — tier 1 / 2 / 3 (all three)
    MACD:            Two-tier post-cross window ±5 (fresh 0-1d) / ±3 (carry 2-5d)
                     for both gold and death events, all three timeframes
    OBV:             Two-tier post-cross window ±5 (fresh 0-1d) / ±3 (carry 2-5d)
                     for staircase signal_up / signal_down events, all three
    Wave:            Event ±5/±3 on tip_breakout_up/down, wave_d4_gold/death,
                     sink_reversal (medium card only — wave is medium-period)
    Donchian:        Two-tier event ±5/±3 on entry_long/entry_short with Lucas
                     windows 18/47/123 (mid-gap between SMA periods)
    Distance:        Continuous z-score of (close - SMA(N)) / rolling_std,
                     ±10 cap, breaks bottom-cluster ties (medium SMA34, long SMA144)
    """
    board = ScoreBoard("技術評分")
    _add_turn_rules(board)
    _add_sort_rules(board)
    breadth = _load_breadth_trends()
    _add_breadth_rules(board, breadth)
    _add_flood_rules(board)
    _add_macd_event_rules(board)
    _add_obv_event_rules(board)
    _add_wave_event_rules(board)
    _add_donchian_event_rules(board)
    _add_distance_rules(board)
    return board


def _add_turn_rules(board: ScoreBoard) -> None:
    """Add turn-point scoring rules to medium / long timeframes.

    扣抵_short removed 2026-04-28 (full universe). 2026-05-13 re-tested
    under ~dead: H=60 full -0.058pp / bull -0.087pp drag, bear +0.039pp
    hedge — gain/loss 0.67 insufficient. Permanently rejected.
    """
    w_med = _w("扣抵", "medium")
    w_lng = _w("扣抵", "long")
    for p in MEDIUM_PERIODS:
        _add_turn_pair(board.medium, p, 5 * w_med, "扣抵")
    _add_fuzzy(board.medium, MEDIUM_PERIODS,
               [(13, 2.5 * w_med), (89, 2.5 * w_med)], "扣抵")

    # 扣抵_long upgraded ±5 → ±15 (2026-05-13 adopted under ~dead universe).
    # See project_score_ablation_findings.md — combo with 洪量 ±10 yields
    # H=60 full +0.319pp, bear +0.041pp (no regime hurt).
    for p in LONG_PERIODS:
        _add_turn_pair(board.long, p, 15 * w_lng, "扣抵")
    _add_fuzzy(board.long, LONG_PERIODS, [(89, 15 * w_lng)], "扣抵")


SORT_LABELS = ("medium", "long")


def _add_sort_rules(board: ScoreBoard) -> None:
    """Add sort alignment + forming scoring rules to all three timeframes."""
    cards = {"short": board.short, "medium": board.medium, "long": board.long}

    for label in SORT_LABELS:
        card = cards[label]
        w = _w("排列", label)

        # sort_normal: ±10 × multiplier
        _add_sort_pair(card, "sort_normal", label, 10 * w, "排列")

        # sort_forming: ±5 × multiplier
        _add_forming_pair(card, label, 5 * w, "排列")

    # v298/v299 試驗封存 (2026-05-28, ScoreBoard asymmetric short_sort_normal空排)：
    #   v298 (+10 pts to short side only): Portfolio PF -0.0019 ≈ 噪音, 8069 0 fire
    #   v299 (+30 pts, 3x 加重): Portfolio PF -0.0056, buy_flee PF -0.43 / sell_flee -0.19 大退
    #   8069 仍 0 fire (touch/buy_flee) — 真正擋 sell 的是 OBV gate, 擋 buy_flee 是 prev1<=0
    #   結論：ScoreBoard 強度不是 8069 的瓶頸，sp 加敏感反而破壞 buy_flee/sell_flee gate 校準


def _add_turn_pair(card: ScoreCard, period: int, pts: float, cat: str):
    """Add bullish/bearish turn conditions for one MA period to both sides."""
    # 漲扣抵: long +pts, short -pts
    card.add_long(bool_score(
        f"{period}MA漲扣", pts,
        lambda d, i, p=period: d.close_result.turn[p][i] == 2, cat,
    ))
    card.add_short(bool_score(
        f"{period}MA漲扣", -pts,
        lambda d, i, p=period: d.close_result.turn[p][i] == 2, cat,
    ))
    # 跌扣抵: long -pts, short +pts
    card.add_long(bool_score(
        f"{period}MA跌扣", -pts,
        lambda d, i, p=period: d.close_result.turn[p][i] == 0, cat,
    ))
    card.add_short(bool_score(
        f"{period}MA跌扣", pts,
        lambda d, i, p=period: d.close_result.turn[p][i] == 0, cat,
    ))


def _add_fuzzy(
    card: ScoreCard,
    core_periods: tuple[int, ...],
    fuzzy_specs: list[tuple[int, float]],
    cat: str,
):
    """Add fuzzy-zone conditions that fire only when all core periods are neutral."""
    def _all_neutral(data, i, periods=core_periods):
        return all(data.close_result.turn[p][i] == 1 for p in periods)

    for fz_period, fz_pts in fuzzy_specs:
        # Fuzzy 漲扣: long +pts, short -pts
        card.add_long(bool_score(
            f"{fz_period}MA模糊漲扣", fz_pts,
            lambda d, i, fp=fz_period: _all_neutral(d, i) and d.close_result.turn[fp][i] == 2,
            cat,
        ))
        card.add_short(bool_score(
            f"{fz_period}MA模糊漲扣", -fz_pts,
            lambda d, i, fp=fz_period: _all_neutral(d, i) and d.close_result.turn[fp][i] == 2,
            cat,
        ))
        # Fuzzy 跌扣: long -pts, short +pts
        card.add_long(bool_score(
            f"{fz_period}MA模糊跌扣", -fz_pts,
            lambda d, i, fp=fz_period: _all_neutral(d, i) and d.close_result.turn[fp][i] == 0,
            cat,
        ))
        card.add_short(bool_score(
            f"{fz_period}MA模糊跌扣", fz_pts,
            lambda d, i, fp=fz_period: _all_neutral(d, i) and d.close_result.turn[fp][i] == 0,
            cat,
        ))


def _add_sort_pair(
    card: ScoreCard, sort_type: str, label: str, pts: float, cat: str,
):
    """Add up/down sort alignment conditions for one sort group."""
    # Up alignment: long +pts, short -pts
    card.add_long(bool_score(
        f"{label}_{sort_type}多排", pts,
        lambda d, i, st=sort_type, lb=label: getattr(d.close_result.ma, st)[lb].up[i],
        cat,
    ))
    card.add_short(bool_score(
        f"{label}_{sort_type}多排", -pts,
        lambda d, i, st=sort_type, lb=label: getattr(d.close_result.ma, st)[lb].up[i],
        cat,
    ))
    # Down alignment: long -pts, short +pts
    card.add_long(bool_score(
        f"{label}_{sort_type}空排", -pts,
        lambda d, i, st=sort_type, lb=label: getattr(d.close_result.ma, st)[lb].down[i],
        cat,
    ))
    card.add_short(bool_score(
        f"{label}_{sort_type}空排", pts,
        lambda d, i, st=sort_type, lb=label: getattr(d.close_result.ma, st)[lb].down[i],
        cat,
    ))


def _add_forming_pair(
    card: ScoreCard, label: str, pts: float, cat: str,
):
    """Add forming sort alignment conditions."""
    # Forming up: long +pts, short -pts
    card.add_long(bool_score(
        f"{label}_forming多排", pts,
        lambda d, i, lb=label: d.sort_forming[lb].up[i],
        cat,
    ))
    card.add_short(bool_score(
        f"{label}_forming多排", -pts,
        lambda d, i, lb=label: d.sort_forming[lb].up[i],
        cat,
    ))
    # Forming down: long -pts, short +pts
    card.add_long(bool_score(
        f"{label}_forming空排", -pts,
        lambda d, i, lb=label: d.sort_forming[lb].down[i],
        cat,
    ))
    card.add_short(bool_score(
        f"{label}_forming空排", pts,
        lambda d, i, lb=label: d.sort_forming[lb].down[i],
        cat,
    ))


# ── Flood Reference ────────────────────────────────────────────────────────

FLOOD_TIER_MAP = (
    ("short", 1),
    ("medium", 2),
    ("long", 3),
)


def _add_flood_rules(board: ScoreBoard) -> None:
    """Add flood-reference scoring: ±10 × multiplier per timeframe based on above/below tier k.

    Weight reduced ±15 → ±10 on 2026-05-13 after ~dead-universe ablation
    showed 洪量 family was the heaviest drag (H=60 full -0.30pp) — halving
    weight preserves H=60 bear hedge while cutting bull/full drag.
    """
    cards = {"short": board.short, "medium": board.medium, "long": board.long}
    for scope, tier in FLOOD_TIER_MAP:
        card = cards[scope]
        pts = 10 * _w("洪量", scope)
        # 站上 k 階洪: long +pts, short -pts
        card.add_long(bool_score(
            f"站上{tier}階洪", pts,
            lambda d, i, k=tier: bool(d.volume_result.above_tier[k][i]),
            "洪量",
        ))
        card.add_short(bool_score(
            f"站上{tier}階洪", -pts,
            lambda d, i, k=tier: bool(d.volume_result.above_tier[k][i]),
            "洪量",
        ))
        # 跌破 k 階洪: long -pts, short +pts
        card.add_long(bool_score(
            f"跌破{tier}階洪", -pts,
            lambda d, i, k=tier: bool(d.volume_result.below_tier[k][i]),
            "洪量",
        ))
        card.add_short(bool_score(
            f"跌破{tier}階洪", pts,
            lambda d, i, k=tier: bool(d.volume_result.below_tier[k][i]),
            "洪量",
        ))

    # v194/v194b reverted (pts=10/pts=5 both net negative). prev_flood_high/low
    # arrays kept in volume_result for direct condition use (e.g. sell_flee v195).


# ── Breadth vs Stock Sort ─────────────────────────────────────────────────

# (trend_codes, stock_sort_state, long_side_pts)
# stock_sort_state: "up" = sort_normal.up, "down" = sort_normal.down, "none" = neither
# short_side_pts = -long_side_pts
BREADTH_LONG_RULES: list[tuple[set[int], str, float]] = [
    ({2}, "down", -10),           # strong_bull + stock down
    ({2}, "none", -5),            # strong_bull + no alignment
    ({1, 3}, "down", -5),         # bull/bull_exhausting + stock down
    ({-1, -3}, "up", 5),          # bear/bear_exhausting + stock up
    ({-2}, "up", 10),             # strong_bear + stock up
    ({-2}, "none", 5),            # strong_bear + no alignment
]

BREADTH_SCOPES = (
    ("medium", "medium_trend"),
    ("long", "long_trend"),
)


def _load_breadth_trends() -> dict[date, dict[str, int]]:
    """Load market breadth trends from DB, keyed by date."""
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date, short_trend, medium_trend, long_trend "
            "FROM tw.market_breadth ORDER BY trade_date"
        )
        return {
            r["trade_date"]: {
                "short_trend": r["short_trend"],
                "medium_trend": r["medium_trend"],
                "long_trend": r["long_trend"],
            }
            for r in cur.fetchall()
        }


def _add_breadth_rules(
    board: ScoreBoard,
    breadth: dict[date, dict[str, int]],
) -> None:
    """Add breadth-vs-stock-sort scoring rules."""
    cards = {"short": board.short, "medium": board.medium, "long": board.long}

    for scope, trend_col in BREADTH_SCOPES:
        card = cards[scope]
        w = _w("大盤", scope)
        for trend_codes, sort_state, long_pts in BREADTH_LONG_RULES:
            name = f"大盤{scope}_{sort_state}_{long_pts:+.0f}"
            _add_breadth_item(card, name, long_pts * w, breadth, trend_col, scope, sort_state, trend_codes)


def _add_breadth_item(
    card: ScoreCard,
    name: str,
    long_pts: float,
    breadth: dict[date, dict[str, int]],
    trend_col: str,
    scope: str,
    sort_state: str,
    trend_codes: set[int],
):
    """Add one breadth condition to both long and short sides."""
    def _check(data, i, _tc=trend_col, _sc=scope, _ss=sort_state, _codes=trend_codes):
        day = data.dates[i]
        bt = breadth.get(day)
        if bt is None:
            return False
        if bt[_tc] not in _codes:
            return False
        sn = data.close_result.ma.sort_normal[_sc]
        if _ss == "up":
            return bool(sn.up[i])
        elif _ss == "down":
            return bool(sn.down[i])
        else:  # "none"
            return not sn.up[i] and not sn.down[i]

    card.add_long(bool_score(name, long_pts, _check, "大盤"))
    card.add_short(bool_score(name, -long_pts, _check, "大盤"))


# ── MACD (mirror Go BuyTrendLeverageValue line 7106-7200) ─────────────────


def _add_macd_rules(board: ScoreBoard) -> None:
    """Add MACD direction scoring (short / medium / long).

    Each scope adds a (full ±10, partial ±5) pair on each side, mutually
    exclusive within the outer gate. Outer gate (Go line 7106-7109):
        OSCDirection AND not(WeakI[i,i-1,i-2] OR WeakII[i,i-1] OR WeakIII[i])
    Inner branch:
        WeakI[i,i-1] OR WeakII[i] -> ±5  (mild OSCX weakening, partial credit)
        else                       -> ±10 (clean direction, full credit)
    Mirror for short side with ~OSCDirection and down_weak{1,2,3}.
    """
    cards = {"short": board.short, "medium": board.medium, "long": board.long}
    for scope, card in cards.items():
        _add_macd_scope(card, scope)


def _add_macd_scope(card: ScoreCard, scope: str) -> None:
    label_long_full = f"{scope}MACD多方"
    label_long_partial = f"{scope}MACD多方弱化"
    label_short_full = f"{scope}MACD空方"
    label_short_partial = f"{scope}MACD空方弱化"

    # Long side: OSCDirection True branch
    card.add_long(bool_score(
        label_long_full, 10,
        lambda d, i, s=scope: _macd_long_full(d, i, s),
        "MACD",
    ))
    card.add_long(bool_score(
        label_long_partial, 5,
        lambda d, i, s=scope: _macd_long_partial(d, i, s),
        "MACD",
    ))
    # Short-side mirror on long ScoreCard side: OSCDirection False -> long penalty
    card.add_long(bool_score(
        label_short_full, -10,
        lambda d, i, s=scope: _macd_short_full(d, i, s),
        "MACD",
    ))
    card.add_long(bool_score(
        label_short_partial, -5,
        lambda d, i, s=scope: _macd_short_partial(d, i, s),
        "MACD",
    ))

    # Short side mirrors (swap signs)
    card.add_short(bool_score(
        label_long_full, -10,
        lambda d, i, s=scope: _macd_long_full(d, i, s),
        "MACD",
    ))
    card.add_short(bool_score(
        label_long_partial, -5,
        lambda d, i, s=scope: _macd_long_partial(d, i, s),
        "MACD",
    ))
    card.add_short(bool_score(
        label_short_full, 10,
        lambda d, i, s=scope: _macd_short_full(d, i, s),
        "MACD",
    ))
    card.add_short(bool_score(
        label_short_partial, 5,
        lambda d, i, s=scope: _macd_short_partial(d, i, s),
        "MACD",
    ))


def _macd_outer_gate_long(d: "StockData", i: int, scope: str) -> bool:
    """OSCDirection True AND not(strong-weakening triplet)."""
    osc = getattr(d.macd, scope)
    if not bool(osc.osc_direction[i]):
        return False
    w1 = osc.osc_status_up_weak1
    w2 = osc.osc_status_up_weak2
    w3 = osc.osc_status_up_weak3
    weak1_3day = i >= 2 and bool(w1[i]) and bool(w1[i - 1]) and bool(w1[i - 2])
    weak2_2day = i >= 1 and bool(w2[i]) and bool(w2[i - 1])
    weak3_today = bool(w3[i])
    return not (weak1_3day or weak2_2day or weak3_today)


def _macd_outer_gate_short(d: "StockData", i: int, scope: str) -> bool:
    """OSCDirection False AND not(strong down-weakening triplet)."""
    osc = getattr(d.macd, scope)
    if bool(osc.osc_direction[i]):
        return False
    w1 = osc.osc_status_down_weak1
    w2 = osc.osc_status_down_weak2
    w3 = osc.osc_status_down_weak3
    weak1_3day = i >= 2 and bool(w1[i]) and bool(w1[i - 1]) and bool(w1[i - 2])
    weak2_2day = i >= 1 and bool(w2[i]) and bool(w2[i - 1])
    weak3_today = bool(w3[i])
    return not (weak1_3day or weak2_2day or weak3_today)


def _macd_inner_partial_long(d: "StockData", i: int, scope: str) -> bool:
    """Inner partial branch: WeakI[i,i-1] OR WeakII[i]."""
    osc = getattr(d.macd, scope)
    w1 = osc.osc_status_up_weak1
    weak1_2day = i >= 1 and bool(w1[i]) and bool(w1[i - 1])
    weak2_today = bool(osc.osc_status_up_weak2[i])
    return weak1_2day or weak2_today


def _macd_inner_partial_short(d: "StockData", i: int, scope: str) -> bool:
    osc = getattr(d.macd, scope)
    w1 = osc.osc_status_down_weak1
    weak1_2day = i >= 1 and bool(w1[i]) and bool(w1[i - 1])
    weak2_today = bool(osc.osc_status_down_weak2[i])
    return weak1_2day or weak2_today


def _macd_long_partial(d: "StockData", i: int, scope: str) -> bool:
    return _macd_outer_gate_long(d, i, scope) and _macd_inner_partial_long(d, i, scope)


def _macd_long_full(d: "StockData", i: int, scope: str) -> bool:
    return _macd_outer_gate_long(d, i, scope) and not _macd_inner_partial_long(d, i, scope)


def _macd_short_partial(d: "StockData", i: int, scope: str) -> bool:
    return _macd_outer_gate_short(d, i, scope) and _macd_inner_partial_short(d, i, scope)


def _macd_short_full(d: "StockData", i: int, scope: str) -> bool:
    return _macd_outer_gate_short(d, i, scope) and not _macd_inner_partial_short(d, i, scope)


# ── MACD event-window scoring (cross-day decay, replaces osc_direction) ───
#
# Pure cross-day events miss the post-cross strength run; latched OSC state
# falls into the OBV failure mode (50%+ trigger rate, no cross-sectional
# differentiation). Sweet spot: fire for a short window after the cross,
# decaying to zero. Two-tier approximation:
#   fresh (within last 1 bar): ±5
#   carry (within last 5 bars but not fresh): ±3
# Both tiers are mutually exclusive within each direction (gold vs death).


_MACD_FRESH_WINDOW = 1   # bars (today + yesterday counted as fresh = 0-1d ago)
_MACD_CARRY_WINDOW = 5   # bars (carry covers up to 5d ago)


def _gold_within(d: "StockData", i: int, scope: str, n: int) -> bool:
    """True if macd_gold fired anywhere in [i-n .. i] (inclusive both ends).

    n=1 means today or yesterday (2 bars), n=5 means today through 5 days
    ago (6 bars). Mirrors a backward-looking event-extension window.
    """
    arr = getattr(d.macd, scope).macd_gold
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


def _death_within(d: "StockData", i: int, scope: str, n: int) -> bool:
    arr = getattr(d.macd, scope).macd_death
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


def _add_macd_event_rules(board: ScoreBoard) -> None:
    """Add MACD gold/death event-window rules to all three timeframes."""
    cards = {"short": board.short, "medium": board.medium, "long": board.long}
    for scope, card in cards.items():
        _add_macd_event_scope(card, scope)


def _add_macd_event_scope(card: ScoreCard, scope: str) -> None:
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    w = _w("MACD", scope)
    p_fresh, p_carry = 5 * w, 3 * w

    def gold_fresh(d, i, s=scope, n=fresh):
        return _gold_within(d, i, s, n)

    def gold_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _gold_within(d, i, s, cr) and not _gold_within(d, i, s, fr)

    def death_fresh(d, i, s=scope, n=fresh):
        return _death_within(d, i, s, n)

    def death_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _death_within(d, i, s, cr) and not _death_within(d, i, s, fr)

    # long-side scoring
    card.add_long(bool_score(f"{scope}金叉_新", p_fresh, gold_fresh, "MACD"))
    card.add_long(bool_score(f"{scope}金叉_續", p_carry, gold_carry, "MACD"))
    card.add_long(bool_score(f"{scope}死叉_新", -p_fresh, death_fresh, "MACD"))
    card.add_long(bool_score(f"{scope}死叉_續", -p_carry, death_carry, "MACD"))

    # short-side mirror (sign flipped)
    card.add_short(bool_score(f"{scope}金叉_新", -p_fresh, gold_fresh, "MACD"))
    card.add_short(bool_score(f"{scope}金叉_續", -p_carry, gold_carry, "MACD"))
    card.add_short(bool_score(f"{scope}死叉_新", p_fresh, death_fresh, "MACD"))
    card.add_short(bool_score(f"{scope}死叉_續", p_carry, death_carry, "MACD"))


# ── OBV event-window scoring (mirror of MACD events on staircase signals) ──
#
# Same two-tier window design as _add_macd_event_rules. The previously
# rejected OBV cells used `trend` (latched +1/-1/0) which fell into the
# OBV-class fail mode (100% station, redundant across timeframes). This
# variant uses signal_up / signal_down (staircase line cross events,
# trigger rate 3-5% per direction per scope) — pure event-form.


def _obv_up_within(d: "StockData", i: int, scope: str, n: int) -> bool:
    arr = getattr(d.obv, scope).signal_up
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


def _obv_down_within(d: "StockData", i: int, scope: str, n: int) -> bool:
    arr = getattr(d.obv, scope).signal_down
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


def _add_obv_event_rules(board: ScoreBoard) -> None:
    cards = {"short": board.short, "medium": board.medium, "long": board.long}
    for scope, card in cards.items():
        _add_obv_event_scope(card, scope)


def _add_obv_event_scope(card: ScoreCard, scope: str) -> None:
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    w = _w("OBV", scope)
    p_fresh, p_carry = 5 * w, 3 * w

    def up_fresh(d, i, s=scope, n=fresh):
        return _obv_up_within(d, i, s, n)

    def up_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _obv_up_within(d, i, s, cr) and not _obv_up_within(d, i, s, fr)

    def down_fresh(d, i, s=scope, n=fresh):
        return _obv_down_within(d, i, s, n)

    def down_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _obv_down_within(d, i, s, cr) and not _obv_down_within(d, i, s, fr)

    # long-side scoring
    card.add_long(bool_score(f"{scope}OBV升_新", p_fresh, up_fresh, "OBV"))
    card.add_long(bool_score(f"{scope}OBV升_續", p_carry, up_carry, "OBV"))
    card.add_long(bool_score(f"{scope}OBV降_新", -p_fresh, down_fresh, "OBV"))
    card.add_long(bool_score(f"{scope}OBV降_續", -p_carry, down_carry, "OBV"))

    # short-side mirror (sign flipped)
    card.add_short(bool_score(f"{scope}OBV升_新", -p_fresh, up_fresh, "OBV"))
    card.add_short(bool_score(f"{scope}OBV升_續", -p_carry, up_carry, "OBV"))
    card.add_short(bool_score(f"{scope}OBV降_新", p_fresh, down_fresh, "OBV"))
    card.add_short(bool_score(f"{scope}OBV降_續", p_carry, down_carry, "OBV"))


# ── Wave event-window scoring (three scopes, MA-cross events) ────────────
#
# Round 4: three-scope MA-cross events with truly separated fast/slow MAs
# (mirror of OBV retiering insight — speeds must really differ across
# scopes or three cells become collinear).
#
# Scope mapping:
#   short:  wave_d2_ma cross wave_d4_ma  (existing wave_d4_gold/death)
#   medium: wave_d3_ma cross wave_d6_ma  (new wave_d6_gold/death)
#   long:   wave_d4_ma cross wave_d8_ma  (new wave_d8_gold/death)
#
# Trigger rates (2330 sample): 6.1% / 5.5% / 2.5% per direction. Cross-scope
# same-day overlap: 11/7/1% — true scope separation, not共線 cells.
#
# History: round 1 (5 events incl. tip_breakout, all in medium card) saturated
# at 92.95% nonzero; round 2 dropped tip_breakout (61.5%); round 3 dropped
# sink_reversal asymmetric event (50% nonzero, mean -0.005, three positive
# horizons but H=60 bull -0.034). Round 4 extends round 3's clean wave_d4
# pair into three scopes with the new d6 / d8 cross signals.


def _wave_event_within(d: "StockData", i: int, attr: str, n: int) -> bool:
    arr = getattr(d.wave_result, attr)
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


_WAVE_SCOPE_SIGNALS = {
    "short":  ("wave_d4_gold", "wave_d4_death", "浪D4"),
    "medium": ("wave_d6_gold", "wave_d6_death", "浪D6"),
    "long":   ("wave_d8_gold", "wave_d8_death", "浪D8"),
}


def _add_wave_event_rules(board: ScoreBoard) -> None:
    # 波浪_medium dropped 2026-05-13 under ~dead universe: standalone Δ
    # neutral at H=60 full (+0.001), removing yields H=60 bull +0.017 /
    # H=60 bear +0.007 (near pure win on regimes). short keeps bull alpha,
    # long keeps bear hedge.
    cards = {"short": board.short, "long": board.long}
    for scope, card in cards.items():
        gold_attr, death_attr, label_prefix = _WAVE_SCOPE_SIGNALS[scope]
        _add_wave_event_scope(card, scope, gold_attr, death_attr, label_prefix)


def _add_wave_event_scope(card: ScoreCard, scope: str, gold_attr: str,
                          death_attr: str, label_prefix: str) -> None:
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    w = _w("波浪", scope)
    p_fresh, p_carry = 5 * w, 3 * w

    def gold_fresh(d, i, a=gold_attr, n=fresh):
        return _wave_event_within(d, i, a, n)

    def gold_carry(d, i, a=gold_attr, fr=fresh, cr=carry):
        return _wave_event_within(d, i, a, cr) and not _wave_event_within(d, i, a, fr)

    def death_fresh(d, i, a=death_attr, n=fresh):
        return _wave_event_within(d, i, a, n)

    def death_carry(d, i, a=death_attr, fr=fresh, cr=carry):
        return _wave_event_within(d, i, a, cr) and not _wave_event_within(d, i, a, fr)

    card.add_long(bool_score(f"{label_prefix}金_新", p_fresh, gold_fresh, "波浪"))
    card.add_long(bool_score(f"{label_prefix}金_續", p_carry, gold_carry, "波浪"))
    card.add_long(bool_score(f"{label_prefix}死_新", -p_fresh, death_fresh, "波浪"))
    card.add_long(bool_score(f"{label_prefix}死_續", -p_carry, death_carry, "波浪"))

    card.add_short(bool_score(f"{label_prefix}金_新", -p_fresh, gold_fresh, "波浪"))
    card.add_short(bool_score(f"{label_prefix}金_續", -p_carry, gold_carry, "波浪"))
    card.add_short(bool_score(f"{label_prefix}死_新", p_fresh, death_fresh, "波浪"))
    card.add_short(bool_score(f"{label_prefix}死_續", p_carry, death_carry, "波浪"))


# ── Donchian event-window scoring (Lucas-window N-day breakout events) ────
#
# Round 2 of Donchian入板. Round 1 (memory 2026-04-28) tested both
# don_dir_* (latched) and don_evt_* (event) at Turtle (20/55/120) and
# Fibonacci (21/8, 34/13, 55/21) windows — all overlapping SMA periods,
# all failed. Round 2 uses Lucas numbers 18/47/123 (mid-gap between
# Fibonacci SMAs) to break the共線 with既有 MA cells. Same OBV retiering
# insight applied.
#
# Note: Donchian entries have a mathematical subset relation
#   entry_long_long ⊂ entry_long_medium ⊂ entry_long_short
# A 123-day high is automatically a 47-day high and an 18-day high. So
# three cells fire simultaneously on major breakouts — equivalent to
# graduated tier weighting (small +5, medium +10, big +15). May be feature
# (intensity tiering like 洪量) not bug. Ablation will tell.


def _donchian_event_within(d: "StockData", i: int, scope: str, side: str, n: int) -> bool:
    don = getattr(d.donchian, scope)
    arr = don.entry_long if side == "long" else don.entry_short
    lo = max(0, i - n)
    return bool(arr[lo:i + 1].any())


def _add_donchian_event_rules(board: ScoreBoard) -> None:
    """Long-only Fib 233/144. Window sweep on 89/123/144/233/377:
    - Lucas 123 has best 全期 alpha (+0.030pp) but 空頭 -0.010pp
    - Fib 233 has balanced (+0.023pp 全期) AND uniquely positive 空頭 (+0.023pp)
    Selected 233 for 空頭 contribution to offset C-dampened weights' bear drag.
    short/medium dropped due to subset relation共線 (entry_long_long ⊂ ...)."""
    _add_donchian_event_scope(board.long, "long")


def _add_donchian_event_scope(card: ScoreCard, scope: str) -> None:
    fresh, carry = _MACD_FRESH_WINDOW, _MACD_CARRY_WINDOW
    w = _w("Donchian", scope)
    p_fresh, p_carry = 5 * w, 3 * w

    def long_fresh(d, i, s=scope, n=fresh):
        return _donchian_event_within(d, i, s, "long", n)

    def long_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _donchian_event_within(d, i, s, "long", cr) and not _donchian_event_within(d, i, s, "long", fr)

    def short_fresh(d, i, s=scope, n=fresh):
        return _donchian_event_within(d, i, s, "short", n)

    def short_carry(d, i, s=scope, fr=fresh, cr=carry):
        return _donchian_event_within(d, i, s, "short", cr) and not _donchian_event_within(d, i, s, "short", fr)

    label = {"short": "Don21", "medium": "Don55", "long": "Don233"}[scope]

    # long-side
    card.add_long(bool_score(f"{label}破高_新", p_fresh, long_fresh, "Donchian"))
    card.add_long(bool_score(f"{label}破高_續", p_carry, long_carry, "Donchian"))
    card.add_long(bool_score(f"{label}破低_新", -p_fresh, short_fresh, "Donchian"))
    card.add_long(bool_score(f"{label}破低_續", -p_carry, short_carry, "Donchian"))

    # short-side mirror (sign flipped)
    card.add_short(bool_score(f"{label}破高_新", -p_fresh, long_fresh, "Donchian"))
    card.add_short(bool_score(f"{label}破高_續", -p_carry, long_carry, "Donchian"))
    card.add_short(bool_score(f"{label}破低_新", p_fresh, short_fresh, "Donchian"))
    card.add_short(bool_score(f"{label}破低_續", p_carry, short_carry, "Donchian"))


# ── Distance score (continuous z-score of close vs SMA) ──────────────────
#
# Bool cells produce ties at extremes — 156 stocks all hit -125 when all
# bear cells fire simultaneously (memory 2026-04-28). Continuous z-score
# breaks ties: each stock's specific deviation from SMA differs even if
# bool states are identical.
#
# Formula:
#   dev = close - SMA(N)
#   std = rolling_std(dev, 252)
#   z   = dev / std                 # ~[-3, +3] typical
#   pts = clip(z × 3, -10, +10)     # cap at ±10 per scope
#
# Used on medium (SMA 34) and long (SMA 144) ScoreCards. Skipped on short
# scope because short SMAs are too noisy for cross-section ranking.


_DISTANCE_STD_WINDOW = 252  # 1-year rolling std for z-score normalization
_DISTANCE_Z_SCALE = 3.0     # z × 3 → cap at ±3σ ≈ ±9 (close to ±10 cap)
_DISTANCE_CAP = 10.0


def _zscore_close_vs_sma(d: "StockData", i: int, sma_period: int,
                         cap: float = _DISTANCE_CAP) -> float:
    """Compute z-score of (close - SMA(N)) using 1-year rolling std."""
    if i < sma_period + _DISTANCE_STD_WINDOW:
        return 0.0
    sma_arr = d.close_result.ma.sma[sma_period]
    close = d.close
    dev_now = float(close[i] - sma_arr[i])
    lo = i - _DISTANCE_STD_WINDOW + 1
    dev_window = close[lo:i + 1] - sma_arr[lo:i + 1]
    std = float(np.std(dev_window))
    if std <= 0:
        return 0.0
    z = dev_now / std
    return float(np.clip(z * _DISTANCE_Z_SCALE, -cap, cap))


def _add_distance_rules(board: ScoreBoard) -> None:
    """4 distance cells at SMA 55/89/144/233 (production).

    Sweep on 11 Fibonacci SMA periods (3-377) revealed:
    - Short periods (p3-p34): all NEGATIVE contribution at H=60 全期
      (short SMA z-score reflects daily noise, not structural deviation)
    - Sweet spot p55-p233: positive at all H × regime
    - p377 marginally positive 全期 but 空頭 turns slightly negative

    Selected p55/p89/p144/p233 — best balanced (full +0.346pp,
    空頭 +0.290pp). Adding p377 trades +0.02pp 全期 for -0.018pp 空頭
    (net neutral); skipped to keep 空頭 contribution.
    """
    # p377 added 2026-05-13 under ~dead universe (+0.034pp full, +0.013pp bear);
    # was rejected 2026-05-04 in full universe due to bear -0.039 (dead-fish noise)
    for sma_p in (55, 89, 144, 233, 377):
        category = f"距離_p{sma_p}"
        _add_distance_scope(board.long, sma_period=sma_p, category=category)


def _add_distance_scope(card: ScoreCard, sma_period: int, category: str,
                        cap: float = _DISTANCE_CAP) -> None:
    """Add z-score(close, SMA(N)) continuous cell to one ScoreCard."""
    name = f"距SMA{sma_period}"

    def long_eval(d, i, p=sma_period, c=cap):
        return _zscore_close_vs_sma(d, i, p, cap=c)

    def short_eval(d, i, p=sma_period, c=cap):
        return -_zscore_close_vs_sma(d, i, p, cap=c)

    card.add_long(ScoreItem(
        name=name, points=cap,
        evaluate=long_eval, category=category, continuous=True,
    ))
    card.add_short(ScoreItem(
        name=name, points=cap,
        evaluate=short_eval, category=category, continuous=True,
    ))


def threshold_score(
    name: str,
    points: float,
    accessor: Callable[[StockData, int], float],
    op: str,
    value: float,
    category: str = "",
) -> ScoreItem:
    """Score when a numeric value meets a threshold."""
    import operator
    ops = {">": operator.gt, "<": operator.lt,
           ">=": operator.ge, "<=": operator.le, "==": operator.eq}
    cmp = ops[op]

    def _eval(data: StockData, i: int) -> bool:
        return bool(cmp(accessor(data, i), value))

    return ScoreItem(name=name, points=points, evaluate=_eval, category=category)
