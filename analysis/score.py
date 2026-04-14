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
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from backtest.data import StockData


# ── Timeframe period groups ────────────────────────────────────────────────

SHORT_PERIODS = (3, 5, 8)
MEDIUM_PERIODS = (21, 34, 55)
LONG_PERIODS = (144, 233, 377)


# ── Dataclasses ────────────────────────────────────────────────────────────


@dataclass
class ScoreItem:
    """One scored condition."""
    name: str
    points: float
    evaluate: Callable[[StockData, int], bool]
    category: str = ""


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

    def __init__(self, name: str = "技術評分"):
        self.name = name
        self.short = ScoreCard("短週期")
        self.medium = ScoreCard("中週期")
        self.long = ScoreCard("長週期")

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

    # -- Short timeframe --
    for p in SHORT_PERIODS:
        _add_turn_pair(board.short, p, 5, "扣抵")
    _add_fuzzy(board.short, SHORT_PERIODS, [(13, 5)], "扣抵")

    # -- Medium timeframe --
    for p in MEDIUM_PERIODS:
        _add_turn_pair(board.medium, p, 5, "扣抵")
    _add_fuzzy(board.medium, MEDIUM_PERIODS, [(13, 2.5), (89, 2.5)], "扣抵")

    # -- Long timeframe --
    for p in LONG_PERIODS:
        _add_turn_pair(board.long, p, 5, "扣抵")
    _add_fuzzy(board.long, LONG_PERIODS, [(89, 5)], "扣抵")

    return board


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
