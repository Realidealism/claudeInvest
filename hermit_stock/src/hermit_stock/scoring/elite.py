"""Elite-quality override: rescue structurally dominant companies that fail
a momentum gate (F7/F8) due to natural growth deceleration.

A company qualifies as elite if ANY of these TTM thresholds is met:
    - gross margin    ≥ 40%
    - operating margin ≥ 25%
    - ROE              ≥ 20%
    - FCF margin       ≥ 20%

Use case: 2330 台積電 with score 7/8 (only F8 failed because YoY growth
went from +30% to +20%) — gross margin ~57% triggers the gross-margin
threshold and rescues it from gate exclusion.

The override only fires when:
    - score_full >= 7   (top-tier candidate; one-rule away from perfect)
    - failed gates ⊆ {F7, F8}   (only momentum gates failed; F6 must pass)
    - has_elite_quality(reports) is True
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..data.models import QuarterlyReport

ELITE_GROSS_MARGIN = Decimal("0.40")
ELITE_OP_MARGIN = Decimal("0.25")
ELITE_ROE = Decimal("0.20")
ELITE_FCF_MARGIN = Decimal("0.20")


@dataclass(frozen=True)
class EliteFlags:
    gross_margin_ok: bool
    op_margin_ok: bool
    roe_ok: bool
    fcf_margin_ok: bool

    @property
    def any_ok(self) -> bool:
        return self.gross_margin_ok or self.op_margin_ok or self.roe_ok or self.fcf_margin_ok


def evaluate_elite(reports: list[QuarterlyReport]) -> EliteFlags:
    """Compute the four elite flags for the latest TTM."""
    if len(reports) < 4:
        return EliteFlags(False, False, False, False)
    last4 = reports[-4:]

    rev_sum = sum((r.revenue for r in last4 if r.revenue is not None), Decimal(0))

    gross_ok = False
    op_ok = False
    fcf_ok = False
    if rev_sum > 0:
        if all(r.gross_profit is not None for r in last4):
            gp_sum = sum(
                (r.gross_profit for r in last4 if r.gross_profit is not None),
                Decimal(0),
            )
            gross_ok = (gp_sum / rev_sum) >= ELITE_GROSS_MARGIN

        if all(r.operating_income is not None for r in last4):
            op_sum = sum(
                (r.operating_income for r in last4 if r.operating_income is not None),
                Decimal(0),
            )
            op_ok = (op_sum / rev_sum) >= ELITE_OP_MARGIN

        # FCF: prefer free_cash_flow column, fall back to OCF + capex
        fcf_total = Decimal(0)
        fcf_complete = True
        for r in last4:
            if r.free_cash_flow is not None:
                fcf_total += r.free_cash_flow
            elif r.operating_cash_flow is not None and r.capex is not None:
                fcf_total += r.operating_cash_flow + r.capex
            else:
                fcf_complete = False
                break
        if fcf_complete:
            fcf_ok = (fcf_total / rev_sum) >= ELITE_FCF_MARGIN

    # ROE TTM = sum(net_income last 4) / latest equity_attributable
    roe_ok = False
    if all(r.net_income is not None for r in last4):
        ni_sum = sum((r.net_income for r in last4 if r.net_income is not None), Decimal(0))
        latest = reports[-1]
        equity = latest.equity_attributable or latest.total_equity
        if equity and equity > 0:
            roe_ok = (ni_sum / equity) >= ELITE_ROE

    return EliteFlags(
        gross_margin_ok=gross_ok,
        op_margin_ok=op_ok,
        roe_ok=roe_ok,
        fcf_margin_ok=fcf_ok,
    )


def has_elite_quality(reports: list[QuarterlyReport]) -> bool:
    return evaluate_elite(reports).any_ok
