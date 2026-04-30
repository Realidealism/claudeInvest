"""Forward-EPS extrapolation from monthly-revenue momentum.

Method (per design discussion, plan-(ii)):
    forward_eps = ttm_eps * (1 + cumulative_yoy_ytd of monthly revenue)

Rationale: monthly revenue is a leading indicator (published 10th of each
month, ~45 days ahead of the quarterly report). Multiplying by (1 + YTD YoY)
implicitly assumes net margin stays constant.

The asymmetric design choice (see CLI --forward-eps): when this is enabled,
the snapshot uses `forward_eps` as the per-share numerator but still compares
against the **trailing** 5Y rolling PE band. This gives an interpretation of
"how does today's price compare to historical normal valuation, after giving
the stock credit for forward growth".
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
