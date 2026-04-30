"""Daily PE / PB / PS (TTM), strictly publish_date-aligned.

For each trade_date d, the multiple uses the latest QuarterlyReport whose
publish_date <= d. Per-share numerators (eps_ttm, bvps, sps_ttm) are computed
once per quarter and aligned by forward-fill of publish_date.

Lookahead-bias: this module is the second key chokepoint after
data/as_of.py. The publish_date join is on report.publish_date, never on
report.period_end. Tests in test_valuation.py enforce this.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from ..data.models import DailyPrice, QuarterlyReport


@dataclass(frozen=True)
class QuarterMultiples:
    """Per-share metrics knowable as-of a quarter's publish_date."""

    period: str
    publish_date: pd.Timestamp
    eps_ttm: Decimal | None
    bvps: Decimal | None
    sps_ttm: Decimal | None


def quarter_multiples(reports: list[QuarterlyReport]) -> list[QuarterMultiples]:
    """Compute TTM per-share metrics for each quarter as-of its publish_date.

    Reports must be sorted ascending by period_end (DB adapter does this).
    """
    out: list[QuarterMultiples] = []
    for i, r in enumerate(reports):
        # eps_ttm needs current quarter + 3 prior
        if i >= 3:
            window = reports[i - 3 : i + 1]
            if all(q.eps is not None for q in window):
                eps_ttm: Decimal | None = sum(
                    (q.eps for q in window if q.eps is not None), Decimal(0)
                )
            else:
                eps_ttm = None
        else:
            eps_ttm = None

        if r.book_value_per_share is not None:
            bvps_v = r.book_value_per_share
        elif (
            r.equity_attributable is not None
            and r.shares_outstanding is not None
            and r.shares_outstanding != 0
        ):
            bvps_v = r.equity_attributable / r.shares_outstanding
        elif (
            r.total_equity is not None
            and r.shares_outstanding is not None
            and r.shares_outstanding != 0
        ):
            bvps_v = r.total_equity / r.shares_outstanding
        else:
            bvps_v = None

        if i >= 3 and r.shares_outstanding and r.shares_outstanding != 0:
            window = reports[i - 3 : i + 1]
            if all(q.revenue is not None for q in window):
                rev_ttm = sum((q.revenue for q in window if q.revenue is not None), Decimal(0))
                sps_ttm: Decimal | None = rev_ttm / r.shares_outstanding
            else:
                sps_ttm = None
        else:
            sps_ttm = None

        out.append(
            QuarterMultiples(
                period=r.period,
                publish_date=pd.Timestamp(r.publish_date),
                eps_ttm=eps_ttm,
                bvps=bvps_v,
                sps_ttm=sps_ttm,
            )
        )
    return out


def daily_multiples(
    reports: list[QuarterlyReport],
    prices: list[DailyPrice],
) -> pd.DataFrame:
    """Build a DataFrame indexed by trade_date with columns close/pe/pb/ps.

    The per-share metric used on day d is whichever quarter had the latest
    publish_date <= d.
    """
    if not reports or not prices:
        return pd.DataFrame(columns=["close", "pe", "pb", "ps"])

    qm = quarter_multiples(reports)
    pub_ts = [q.publish_date for q in qm]
    pub_sorted = sorted(pub_ts)
    if pub_sorted != pub_ts:
        # ensure sorted by publish_date
        qm.sort(key=lambda q: q.publish_date)
        pub_ts = [q.publish_date for q in qm]

    rows: list[dict[str, object]] = []
    for p in prices:
        if p.close is None:
            continue
        d = pd.Timestamp(p.trade_date)
        idx = bisect.bisect_right(pub_ts, d) - 1
        if idx < 0:
            rows.append(
                {"trade_date": d, "close": float(p.close), "pe": None, "pb": None, "ps": None}
            )
            continue
        m = qm[idx]
        close = float(p.close)
        pe = float(p.close / m.eps_ttm) if m.eps_ttm and m.eps_ttm != 0 else None
        pb = float(p.close / m.bvps) if m.bvps and m.bvps != 0 else None
        ps = float(p.close / m.sps_ttm) if m.sps_ttm and m.sps_ttm != 0 else None
        rows.append({"trade_date": d, "close": close, "pe": pe, "pb": pb, "ps": ps})

    df = pd.DataFrame(rows).set_index("trade_date").sort_index()
    return df
