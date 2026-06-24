"""Strategy layer (M2): capital split + cash-pool deployment decisions.

Buy-and-accumulate only — no sell/stop logic anywhere. Pure decision helpers
the backtest loop calls each day/month.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CapitalParams:
    monthly_input: float
    base_ratio: float
    pool_cap_months: float
    stale_deploy_months: int
    stale_deploy_frac: float

    @property
    def base_amount(self) -> float:
        return self.monthly_input * self.base_ratio

    @property
    def reserve_amount(self) -> float:
        return self.monthly_input * (1.0 - self.base_ratio)

    @property
    def pool_cap(self) -> float:
        # Pool ceiling expressed as N months of the reserve inflow.
        return self.pool_cap_months * self.reserve_amount


def deploy_fraction(score: float, tiers: list) -> float:
    """Fraction of the current pool to deploy at this cheapness score.

    tiers: ascending [[threshold, frac], ...]. Returns the frac of the highest
    threshold the score clears, else 0.
    """
    frac = 0.0
    for thr, f in tiers:
        if score >= thr:
            frac = f
    return frac


def should_deploy(score: float, is_choppy: bool, threshold: float) -> bool:
    """Deploy only when cheap enough AND not a choppy high-vol washout."""
    return (score >= threshold) and (not is_choppy)
