"""Estimate disclosure dates from statutory deadlines.

Real DB lacks publish_date columns. For Phase 1 we estimate using TWSE/MOPS
statutory caps so all lookahead-bias defenses can use a single, conservative
upper bound. This is intentionally late, not early — using the deadline means
we never accidentally let a report leak into a backtest before it could
actually have been read.

Statutory caps (Taiwan listed companies, post-IFRS):
    Q1 (period_end 3/31)  -> publish by 5/15
    Q2 (period_end 6/30)  -> publish by 8/14
    Q3 (period_end 9/30)  -> publish by 11/14
    Q4 (period_end 12/31) -> publish by 3/31 of next year (annual report)
    Monthly revenue        -> publish by 10th of next month
"""

from __future__ import annotations

from datetime import date


def quarter_publish_date(year: int, quarter: int) -> date:
    if quarter == 1:
        return date(year, 5, 15)
    if quarter == 2:
        return date(year, 8, 14)
    if quarter == 3:
        return date(year, 11, 14)
    if quarter == 4:
        return date(year + 1, 3, 31)
    raise ValueError(f"invalid quarter: {quarter}")


def quarter_period_end(year: int, quarter: int) -> date:
    end_md = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}[quarter]
    return date(year, end_md[0], end_md[1])


def monthly_publish_date(year_month: str) -> date:
    """year_month like '2024-09' -> publish 2024-10-10."""
    y, m = year_month.split("-")
    yi, mi = int(y), int(m)
    if mi == 12:
        return date(yi + 1, 1, 10)
    return date(yi, mi + 1, 10)


def period_label(year: int, quarter: int) -> str:
    return f"{year}Q{quarter}"


def parse_period_label(label: str) -> tuple[int, int]:
    """'2024Q3' -> (2024, 3)."""
    y, q = label.split("Q")
    return int(y), int(q)
