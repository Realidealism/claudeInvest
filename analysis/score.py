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
    _add_turn_rules(board)
    return board


def build_scoreboard() -> ScoreBoard:
    """
    Build a unified ScoreBoard with all technical scoring rules.

    Turn (扣抵):     per-MA ±5, fuzzy conditions when neutral
    Sort (排列):     sort_normal ±10, sort_lp ±10
    Forming (成形):  sort_forming ±5
    Breadth (大盤):  market trend vs stock sort alignment
    Wave (波浪):     wave_trend cumulative ±5 at 0.3/0.5/0.8
    """
    board = ScoreBoard("技術評分")
    _add_turn_rules(board)
    _add_sort_rules(board)
    breadth = _load_breadth_trends()
    _add_breadth_rules(board, breadth)
    _add_wave_trend_rules(board)
    return board


def _add_turn_rules(board: ScoreBoard) -> None:
    """Add turn-point scoring rules to all three timeframes."""
    for p in SHORT_PERIODS:
        _add_turn_pair(board.short, p, 5, "扣抵")
    _add_fuzzy(board.short, SHORT_PERIODS, [(13, 5)], "扣抵")

    for p in MEDIUM_PERIODS:
        _add_turn_pair(board.medium, p, 5, "扣抵")
    _add_fuzzy(board.medium, MEDIUM_PERIODS, [(13, 2.5), (89, 2.5)], "扣抵")

    for p in LONG_PERIODS:
        _add_turn_pair(board.long, p, 5, "扣抵")
    _add_fuzzy(board.long, LONG_PERIODS, [(89, 5)], "扣抵")


SORT_LABELS = ("short", "medium", "long")


def _add_sort_rules(board: ScoreBoard) -> None:
    """Add sort alignment + forming scoring rules to all three timeframes."""
    cards = {"short": board.short, "medium": board.medium, "long": board.long}

    for label in SORT_LABELS:
        card = cards[label]

        # sort_normal: ±10
        _add_sort_pair(card, "sort_normal", label, 10, "排列")

        # sort_forming: ±5
        _add_forming_pair(card, label, 5, "排列")


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


# ── Wave Trend ────────────────────────────────────────────────────────────

WAVE_THRESHOLDS = (0.3, 0.5, 0.8)

WAVE_SCOPES = (
    ("short", "short"),
    ("medium", "medium"),
    ("long", "long"),
)


def _add_wave_trend_rules(board: ScoreBoard) -> None:
    """Add cumulative wave trend scoring: ±5 at each threshold."""
    cards = {"short": board.short, "medium": board.medium, "long": board.long}

    for scope, wt_attr in WAVE_SCOPES:
        card = cards[scope]
        for thresh in WAVE_THRESHOLDS:
            # Bullish: wave_trend > thresh → long +5, short -5
            card.add_long(bool_score(
                f"波浪{scope}>{thresh}", 5,
                lambda d, i, a=wt_attr, t=thresh: float(getattr(d.wave_result.wave_trend, a)[i]) > t,
                "波浪",
            ))
            card.add_short(bool_score(
                f"波浪{scope}>{thresh}", -5,
                lambda d, i, a=wt_attr, t=thresh: float(getattr(d.wave_result.wave_trend, a)[i]) > t,
                "波浪",
            ))
            # Bearish: wave_trend < -thresh → long -5, short +5
            card.add_long(bool_score(
                f"波浪{scope}<{-thresh}", -5,
                lambda d, i, a=wt_attr, t=thresh: float(getattr(d.wave_result.wave_trend, a)[i]) < -t,
                "波浪",
            ))
            card.add_short(bool_score(
                f"波浪{scope}<{-thresh}", 5,
                lambda d, i, a=wt_attr, t=thresh: float(getattr(d.wave_result.wave_trend, a)[i]) < -t,
                "波浪",
            ))


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
    ("short", "short_trend"),
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
        for trend_codes, sort_state, long_pts in BREADTH_LONG_RULES:
            name = f"大盤{scope}_{sort_state}_{long_pts:+.0f}"
            _add_breadth_item(card, name, long_pts, breadth, trend_col, scope, sort_state, trend_codes)


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
