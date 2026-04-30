"""Eight winning-stock financial rules (F1-F8) per design doc §7.

Each rule returns a RuleResult. `passed=None` means "data insufficient" (e.g.
fewer than 8 quarters of history); `passed=True/False` means a definitive
verdict was reached. Score aggregation in scorer.py treats None as 0 points
but reports it as 'unknown' separately.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..data.models import MonthlyRevenue, QuarterlyReport
from ..indicators import cashflow, efficiency, leverage, profitability, revenue


@dataclass(frozen=True)
class RuleResult:
    code: str  # "F1", "F2", ...
    name: str
    passed: bool | None  # None = insufficient data
    value: Decimal | int | None
    threshold: Decimal | int | str | None
    message: str

    @property
    def is_unknown(self) -> bool:
        return self.passed is None


@dataclass(frozen=True)
class Thresholds:
    revenue_growth_yoy: Decimal = Decimal("0.10")
    earnings_growth_yoy: Decimal = Decimal("0.10")
    gross_margin_consecutive_years: int = 2
    inventory_days_max_consecutive_rises: int = 2
    debt_ratio_max_consecutive_year_rises: int = 3
    fcf_max_consecutive_negative_years: int = 3


def f1_earnings_growth(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    yoy = profitability.net_income_yoy_ttm(reports)
    if yoy is None:
        return RuleResult(
            "F1",
            "獲利成長率達標",
            None,
            None,
            t.earnings_growth_yoy,
            "資料不足（需 8 季歷史）",
        )
    passed = yoy >= t.earnings_growth_yoy
    return RuleResult(
        "F1",
        "獲利成長率達標",
        passed,
        yoy,
        t.earnings_growth_yoy,
        f"TTM 淨利 YoY = {yoy * 100:.2f}%",
    )


def f2_revenue_growth(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    yoy = revenue.revenue_yoy_ttm(reports)
    if yoy is None:
        return RuleResult(
            "F2",
            "營收成長率達標",
            None,
            None,
            t.revenue_growth_yoy,
            "資料不足（需 8 季歷史）",
        )
    passed = yoy >= t.revenue_growth_yoy
    return RuleResult(
        "F2",
        "營收成長率達標",
        passed,
        yoy,
        t.revenue_growth_yoy,
        f"TTM 營收 YoY = {yoy * 100:.2f}%",
    )


def f3_gross_margin_rising(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    rises = profitability.gross_margin_consecutive_rises(
        reports, lookback_years=t.gross_margin_consecutive_years + 1
    )
    if rises is None:
        return RuleResult(
            "F3",
            "毛利率連續提升",
            None,
            None,
            t.gross_margin_consecutive_years,
            f"資料不足（需 {t.gross_margin_consecutive_years + 1} 年歷史）",
        )
    passed = rises >= t.gross_margin_consecutive_years
    return RuleResult(
        "F3",
        "毛利率連續提升",
        passed,
        rises,
        t.gross_margin_consecutive_years,
        f"連續上升 {rises} 年（門檻 {t.gross_margin_consecutive_years} 年）",
    )


def f4_inventory_days_not_rising(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    series = efficiency.inventory_days_series(reports)
    clean = [v for v in series if v is not None]
    if len(clean) < 2:
        return RuleResult(
            "F4",
            "存貨週轉天數未惡化",
            None,
            None,
            t.inventory_days_max_consecutive_rises,
            "資料不足（需至少 2 季存貨/cogs）",
        )
    rises = efficiency.consecutive_rises_at_end(series)
    passed = rises < t.inventory_days_max_consecutive_rises
    return RuleResult(
        "F4",
        "存貨週轉天數未惡化",
        passed,
        rises,
        t.inventory_days_max_consecutive_rises,
        f"連續上升 {rises} 季（容忍 {t.inventory_days_max_consecutive_rises - 1} 季）",
    )


def f5_debt_ratio_not_rising(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    rises = leverage.debt_ratio_consecutive_rises_years(
        reports, lookback=t.debt_ratio_max_consecutive_year_rises + 1
    )
    if rises is None:
        return RuleResult(
            "F5",
            "負債比率未惡化",
            None,
            None,
            t.debt_ratio_max_consecutive_year_rises,
            f"資料不足（需 {t.debt_ratio_max_consecutive_year_rises + 1} 年 Q4 資料）",
        )
    passed = rises < t.debt_ratio_max_consecutive_year_rises
    return RuleResult(
        "F5",
        "負債比率未惡化",
        passed,
        rises,
        t.debt_ratio_max_consecutive_year_rises,
        f"連續上升 {rises} 年（容忍 {t.debt_ratio_max_consecutive_year_rises - 1} 年）",
    )


def f6_fcf_healthy(reports: list[QuarterlyReport], t: Thresholds) -> RuleResult:
    series = cashflow.annual_fcf_series(reports)
    if not series:
        return RuleResult(
            "F6",
            "自由現金流量健康",
            None,
            None,
            t.fcf_max_consecutive_negative_years,
            "資料不足（需至少 1 個完整年度 FCF）",
        )
    neg = cashflow.fcf_consecutive_negative_years(reports)
    passed = neg < t.fcf_max_consecutive_negative_years
    return RuleResult(
        "F6",
        "自由現金流量健康",
        passed,
        neg,
        t.fcf_max_consecutive_negative_years,
        f"最近連續為負 {neg} 年（容忍 {t.fcf_max_consecutive_negative_years - 1} 年）",
    )


def f7_monthly_momentum(monthly: list[MonthlyRevenue]) -> RuleResult:
    """最新月 YoY > 累積 YoY 且月營收創 12 個月新高."""
    if len(monthly) < 12:
        return RuleResult(
            "F7",
            "月營收動能",
            None,
            None,
            "—",
            "資料不足（需至少 12 個月歷史）",
        )
    latest_yoy = revenue.latest_month_yoy(monthly)
    cum_yoy = revenue.cumulative_yoy_ytd(monthly)
    is_high = revenue.is_monthly_revenue_12m_high(monthly)
    if latest_yoy is None or cum_yoy is None or is_high is None:
        return RuleResult(
            "F7",
            "月營收動能",
            None,
            None,
            "—",
            "月營收欄位缺漏",
        )
    accel = latest_yoy > cum_yoy
    passed = accel and is_high
    return RuleResult(
        "F7",
        "月營收動能",
        passed,
        (latest_yoy, cum_yoy, is_high),  # type: ignore[arg-type]
        "—",
        f"月 YoY {latest_yoy * 100:.2f}% > 累積 YoY {cum_yoy * 100:.2f}%? "
        f"{'是' if accel else '否'}；12 月新高？{'是' if is_high else '否'}",
    )


def f8_quarterly_momentum(reports: list[QuarterlyReport]) -> RuleResult:
    """最新季 QoQ 為正，且 YoY 高於前一季 YoY."""
    if len(reports) < 6:
        return RuleResult(
            "F8",
            "季營收動能",
            None,
            None,
            "—",
            "資料不足（需至少 6 季歷史）",
        )
    qoq = revenue.quarterly_qoq(reports)
    yoy_now = revenue.quarterly_yoy_at(reports, -1)
    yoy_prev = revenue.quarterly_yoy_at(reports, -2)
    if qoq is None or yoy_now is None or yoy_prev is None:
        return RuleResult(
            "F8",
            "季營收動能",
            None,
            None,
            "—",
            "欄位缺漏",
        )
    passed = qoq > 0 and yoy_now > yoy_prev
    return RuleResult(
        "F8",
        "季營收動能",
        passed,
        (qoq, yoy_now, yoy_prev),  # type: ignore[arg-type]
        "—",
        f"QoQ {qoq * 100:.2f}%；YoY 今 {yoy_now * 100:.2f}% vs 前 {yoy_prev * 100:.2f}%",
    )


def evaluate_all(
    reports: list[QuarterlyReport],
    monthly: list[MonthlyRevenue],
    thresholds: Thresholds | None = None,
) -> list[RuleResult]:
    t = thresholds or Thresholds()
    return [
        f1_earnings_growth(reports, t),
        f2_revenue_growth(reports, t),
        f3_gross_margin_rising(reports, t),
        f4_inventory_days_not_rising(reports, t),
        f5_debt_ratio_not_rising(reports, t),
        f6_fcf_healthy(reports, t),
        f7_monthly_momentum(monthly),
        f8_quarterly_momentum(reports),
    ]
