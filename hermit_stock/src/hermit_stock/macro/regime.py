"""Macro regime classifier.

Five auto-derived signals + a composite Bull/Neutral/Bear label. All inputs
come from data already in tw.* tables — no manual data entry required.

Signals:
    1. taiex_trend       : TAIEX close vs 200-day SMA → 1 (Bull) / -1 (Bear)
    2. taiex_60d_return  : 60-trading-day cumulative return; classified > +5% / -5%
    3. breadth_medium    : tw.market_breadth.medium_trend (-2..+2)
    4. foreign_20d_net   : 20-day rolling sum of total foreign net buy
    5. margin_yoy        : margin balance YoY% — high → leverage risk

Composite label rule:
    Bull   : taiex_trend = +1 AND breadth >= 0 AND foreign_20d >= 0
    Bear   : taiex_trend = -1 AND (breadth <= 0 OR foreign_20d < 0)
    Neutral: otherwise

The rule is intentionally permissive (Bull condition wider than strict Bear)
because we want the macro filter to *kick in* when conditions are clearly
deteriorating, not at every wobble.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

import pandas as pd

Regime = Literal["Bull", "Neutral", "Bear"]
TrendLabel = Literal["Bull", "Bear"]


def taiex_trend(taiex: pd.Series, window: int = 200) -> pd.Series:
    sma = taiex.rolling(window=window, min_periods=window).mean()
    return (taiex > sma).astype(int) - (taiex < sma).astype(int)


def taiex_60d_return(taiex: pd.Series, window: int = 60) -> pd.Series:
    return taiex.pct_change(periods=window)


def foreign_20d_net(foreign_daily: pd.Series, window: int = 20) -> pd.Series:
    return foreign_daily.rolling(window=window, min_periods=1).sum()


def margin_yoy(margin: pd.Series) -> pd.Series:
    """YoY% change. Uses 252 trading-day shift as a proxy for 1 year."""
    return margin.pct_change(periods=252)


@dataclass(frozen=True)
class MacroSnapshot:
    as_of: date
    taiex_close: float | None
    taiex_trend: int  # 1 / 0 / -1
    taiex_60d_return: float | None
    breadth_medium: int | None  # -2..+2
    foreign_20d_net: float | None
    margin_yoy: float | None
    regime: Regime

    def label_taiex_trend(self) -> str:
        return {1: "Bull", 0: "—", -1: "Bear"}.get(self.taiex_trend, "—")

    def label_breadth(self) -> str:
        return {2: "Strong Bull", 1: "Bull", 0: "Neutral", -1: "Bear", -2: "Strong Bear"}.get(
            self.breadth_medium or 0, "—"
        )

    def label_60d(self) -> str:
        if self.taiex_60d_return is None:
            return "—"
        if self.taiex_60d_return > 0.05:
            return f"Hot ({self.taiex_60d_return * 100:+.1f}%)"
        if self.taiex_60d_return < -0.05:
            return f"Cold ({self.taiex_60d_return * 100:+.1f}%)"
        return f"Flat ({self.taiex_60d_return * 100:+.1f}%)"

    def label_foreign(self) -> str:
        if self.foreign_20d_net is None:
            return "—"
        sign = "Inflow" if self.foreign_20d_net > 0 else "Outflow"
        return f"{sign} ({self.foreign_20d_net / 1e9:+.1f}B NTD/20d)"

    def label_margin(self) -> str:
        if self.margin_yoy is None:
            return "—"
        if self.margin_yoy > 0.20:
            return f"High Leverage ({self.margin_yoy * 100:+.1f}%)"
        if self.margin_yoy < 0:
            return f"Deleveraging ({self.margin_yoy * 100:+.1f}%)"
        return f"Normal ({self.margin_yoy * 100:+.1f}%)"


def composite_regime(trend: int, breadth: int | None, foreign_20d: float | None) -> Regime:
    breadth_v = breadth if breadth is not None else 0
    foreign_v = foreign_20d if foreign_20d is not None else 0.0
    if trend == 1 and breadth_v >= 0 and foreign_v >= 0:
        return "Bull"
    if trend == -1 and (breadth_v <= 0 or foreign_v < 0):
        return "Bear"
    return "Neutral"


def snapshot_at(
    as_of: date,
    *,
    taiex: pd.Series,
    breadth_df: pd.DataFrame,
    foreign_daily: pd.Series,
    margin: pd.Series,
) -> MacroSnapshot:
    """Compute all 5 signals + composite at a single as-of date."""
    ts = pd.Timestamp(as_of)

    def _last_at_or_before(s: pd.Series, target: pd.Timestamp) -> object:
        idx = s.index[s.index <= target]
        return s.loc[idx[-1]] if len(idx) > 0 else None

    trend_series = taiex_trend(taiex)
    ret60_series = taiex_60d_return(taiex)
    fnet20_series = foreign_20d_net(foreign_daily)
    margin_yoy_series = margin_yoy(margin)

    trend_v_f = _to_float(_last_at_or_before(trend_series, ts))
    trend_int = int(trend_v_f) if trend_v_f is not None else 0

    ret60 = _last_at_or_before(ret60_series, ts)
    fnet20 = _last_at_or_before(fnet20_series, ts)
    myoy = _last_at_or_before(margin_yoy_series, ts)
    taiex_close = _last_at_or_before(taiex, ts)

    breadth_v: int | None = None
    if not breadth_df.empty and "medium_trend" in breadth_df.columns:
        idx = breadth_df.index[breadth_df.index <= ts]
        if len(idx) > 0:
            v = breadth_df.loc[idx[-1], "medium_trend"]
            breadth_v = int(v) if pd.notna(v) else None

    regime = composite_regime(trend_int, breadth_v, _to_float(fnet20))

    return MacroSnapshot(
        as_of=as_of,
        taiex_close=_to_float(taiex_close),
        taiex_trend=trend_int,
        taiex_60d_return=_to_float(ret60),
        breadth_medium=breadth_v,
        foreign_20d_net=_to_float(fnet20),
        margin_yoy=_to_float(myoy),
        regime=regime,
    )


def regime_series(
    *,
    taiex: pd.Series,
    breadth_df: pd.DataFrame,
    foreign_daily: pd.Series,
) -> pd.Series:
    """Per-day regime label series, indexed by trade_date.

    Used by the backtest macro filter — for each rebalance date d, look up the
    regime in this series.
    """
    if taiex.empty:
        return pd.Series(dtype=object)
    trend = taiex_trend(taiex)
    fnet20 = foreign_20d_net(foreign_daily)
    breadth = (
        breadth_df["medium_trend"]
        if not breadth_df.empty and "medium_trend" in breadth_df.columns
        else pd.Series(dtype=int)
    )
    df = pd.concat(
        [trend.rename("trend"), breadth.rename("breadth"), fnet20.rename("fnet20")],
        axis=1,
    ).sort_index()
    df = df.ffill()

    def _row_regime(row: pd.Series) -> Regime:
        return composite_regime(
            int(row["trend"]) if pd.notna(row["trend"]) else 0,
            int(row["breadth"]) if pd.notna(row["breadth"]) else None,
            float(row["fnet20"]) if pd.notna(row["fnet20"]) else None,
        )

    return df.apply(_row_regime, axis=1)


def _to_float(v: object) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
