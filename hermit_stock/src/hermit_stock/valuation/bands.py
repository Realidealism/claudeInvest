"""Rolling-window mean ± 1σ ± 2σ bands for PE / PB / PS series."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class Band:
    mean: float | None
    std: float | None
    minus_2sd: float | None
    minus_1sd: float | None
    plus_1sd: float | None
    plus_2sd: float | None
    n_obs: int

    @classmethod
    def from_series(cls, s: pd.Series) -> Band:
        clean = s.dropna()
        n = int(len(clean))
        if n < 60:
            return cls(None, None, None, None, None, None, n)
        mean = float(clean.mean())
        std = float(clean.std(ddof=0))
        return cls(
            mean=mean,
            std=std,
            minus_2sd=mean - 2 * std,
            minus_1sd=mean - std,
            plus_1sd=mean + std,
            plus_2sd=mean + std * 2,
            n_obs=n,
        )

    def classify(self, current: float | None) -> str | None:
        if current is None or self.mean is None or self.std is None:
            return None
        if current < self.minus_2sd:  # type: ignore[operator]
            return "< −2σ"
        if current < self.minus_1sd:  # type: ignore[operator]
            return "−2σ ~ −1σ"
        if current < self.mean:
            return "−1σ ~ mean"
        if current < self.plus_1sd:  # type: ignore[operator]
            return "mean ~ +1σ"
        if current < self.plus_2sd:  # type: ignore[operator]
            return "+1σ ~ +2σ"
        return "> +2σ"


def rolling_band(
    daily: pd.DataFrame,
    column: str,
    window_years: int = 5,
    as_of: pd.Timestamp | None = None,
) -> Band:
    """Compute mean/std band for `column` over the last `window_years` of data
    ending at `as_of` (inclusive). If as_of is None, uses the latest row.
    """
    if column not in daily.columns or daily.empty:
        return Band(None, None, None, None, None, None, 0)
    end = as_of if as_of is not None else daily.index.max()
    start = end - pd.DateOffset(years=window_years)
    window = daily.loc[(daily.index >= start) & (daily.index <= end), column]
    return Band.from_series(window)
