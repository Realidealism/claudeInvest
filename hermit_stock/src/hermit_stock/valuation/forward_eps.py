"""Forward EPS / SPS extrapolation from monthly-revenue momentum.

Both forward metrics use the same growth driver — YTD-cumulative monthly
revenue YoY — under the assumption that the next 12 months will track the
trailing year's growth pace:

    forward_eps = ttm_eps * (1 + cum_yoy)   # assumes net margin stable
    forward_sps = ttm_sps * (1 + cum_yoy)   # direct revenue scaling

Rationale: monthly revenue is a leading indicator (published 10th of each
month, ~45 days ahead of quarterly reports). The asymmetric design choice
(see CLI --forward-eps): the snapshot uses the forward metric as numerator
but still compares against the **trailing** 5Y rolling PE/PS band, giving
the interpretation of "how does today's price compare to historical normal
valuation, after giving the stock credit for forward growth".

We skip forward_bvps deliberately — book value is for asset-heavy companies
(banks, REITs, mature manufacturers) where the strategy's growth/momentum
focus already disqualifies most names.
"""

from __future__ import annotations

from decimal import Decimal

from ..data.models import MonthlyRevenue, QuarterlyReport
from ..indicators import per_share, revenue


def forward_eps_revenue_momentum(
    reports: list[QuarterlyReport], monthly: list[MonthlyRevenue]
) -> Decimal | None:
    """Project EPS forward by scaling TTM EPS by YTD-cumulative revenue YoY.

    Returns None if either component is missing or if cum_yoy is below -100%
    (which would yield a non-positive forward EPS).
    """
    ttm_eps = per_share.eps_ttm(reports)
    if ttm_eps is None:
        return None
    cum_yoy = revenue.cumulative_yoy_ytd(monthly)
    if cum_yoy is None:
        return None
    factor = Decimal(1) + cum_yoy
    if factor <= 0:
        return None
    return ttm_eps * factor


def forward_sps_revenue_momentum(
    reports: list[QuarterlyReport], monthly: list[MonthlyRevenue]
) -> Decimal | None:
    """Project sales-per-share forward by direct revenue extrapolation.

    forward_sps = ttm_sps * (1 + cum_yoy)
    """
    ttm_sps = per_share.sps_ttm(reports)
    if ttm_sps is None:
        return None
    cum_yoy = revenue.cumulative_yoy_ytd(monthly)
    if cum_yoy is None:
        return None
    factor = Decimal(1) + cum_yoy
    if factor <= 0:
        return None
    return ttm_sps * factor
