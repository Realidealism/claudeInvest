"""Phase 2 analyzer: indicators + scoring + valuation, lookahead-bias safe."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd

from ..data.as_of import filter_monthly, filter_prices, filter_quarterly
from ..data.models import DailyPrice, MonthlyRevenue, QuarterlyReport, StockMeta
from ..data.publish_date import parse_period_label, quarter_publish_date
from ..indicators import cashflow, efficiency, leverage, per_share, profitability, revenue
from ..macro.regime import MacroSnapshot
from ..scoring.rules import RuleResult, evaluate_all
from ..scoring.scorer import Scoreboard, score
from ..valuation.bands import rolling_band
from ..valuation.methods import ValuationSnapshot, make_snapshot
from ..valuation.multiples import daily_multiples
from ..valuation.selector import select_valuation_method


def _fmt(v: object, pct: bool = False) -> str:
    if v is None:
        return "—"
    if isinstance(v, Decimal):
        return f"{v * 100:.2f}%" if pct else f"{v:,.2f}"
    if isinstance(v, float):
        return f"{v * 100:.2f}%" if pct else f"{v:,.2f}"
    return str(v)


def _resolve_as_of(label: str) -> date:
    if "Q" in label:
        y, q = parse_period_label(label)
        return quarter_publish_date(y, q)
    return date.fromisoformat(label)


def _rule_row(r: RuleResult) -> str:
    if r.passed is True:
        mark = "✓"
    elif r.passed is False:
        mark = "✗"
    else:
        mark = "?"
    return f"| {r.code} | {r.name} | {mark} | {r.message} |"


def render_phase2_report(
    ticker: str,
    meta: StockMeta | None,
    quarterly: list[QuarterlyReport],
    monthly: list[MonthlyRevenue],
    prices: list[DailyPrice],
    as_of_label: str,
    macro: MacroSnapshot | None = None,
    use_forward_eps: bool = False,
) -> str:
    from ..valuation.forward_eps import forward_eps_revenue_momentum

    as_of = _resolve_as_of(as_of_label)
    qs = filter_quarterly(quarterly, as_of)
    ms = filter_monthly(monthly, as_of)
    ps = filter_prices(prices, as_of)

    name = meta.name if meta else "(unknown)"
    industry = meta.industry if meta and meta.industry else "—"

    rule_results = evaluate_all(qs, ms)
    sb = score(rule_results)

    method = select_valuation_method(qs)
    daily = daily_multiples(qs, ps) if qs and ps else pd.DataFrame()
    band = rolling_band(daily, method.lower(), window_years=5) if not daily.empty else None
    forward_metric_value: float | None = None
    if use_forward_eps:
        from ..valuation.forward_eps import forward_sps_revenue_momentum

        if method == "PE":
            fv = forward_eps_revenue_momentum(qs, ms)
        elif method == "PS":
            fv = forward_sps_revenue_momentum(qs, ms)
        else:
            fv = None  # PB: no forward
        if fv is not None:
            forward_metric_value = float(fv)
    snapshot: ValuationSnapshot | None = (
        make_snapshot(daily, method, band, forward_metric=forward_metric_value)
        if band is not None
        else None
    )

    return _render(
        ticker=ticker,
        name=name,
        industry=industry,
        as_of_label=as_of_label,
        as_of=as_of,
        quarterly_total=len(quarterly),
        monthly_total=len(monthly),
        price_total=len(prices),
        qs=qs,
        ms=ms,
        ps=ps,
        sb=sb,
        snapshot=snapshot,
        macro=macro,
    )


def _render(
    *,
    ticker: str,
    name: str,
    industry: str,
    as_of_label: str,
    as_of: date,
    quarterly_total: int,
    monthly_total: int,
    price_total: int,
    qs: list[QuarterlyReport],
    ms: list[MonthlyRevenue],
    ps: list[DailyPrice],
    sb: Scoreboard,
    snapshot: ValuationSnapshot | None,
    macro: MacroSnapshot | None = None,
) -> str:
    lines: list[str] = []
    lines.append(f"# {ticker} {name} 贏勢股分析報告（截至 {as_of_label}，as_of={as_of}）")
    lines.append("")
    lines.append(f"- 產業：{industry}")
    lines.append(
        f"- 可用資料：季報 {len(qs)}/{quarterly_total}，"
        f"月營收 {len(ms)}/{monthly_total}，日股價 {len(ps)}/{price_total}"
    )
    lines.append(f"- **評等：{sb.grade}（{sb.score}/8 分，未知 {sb.unknown_count}）**")
    lines.append("")

    lines.append("## Step 1–3 全球經濟、資金面、景氣循環")
    lines.append("")
    if macro is None:
        lines.append("> ⚠️ 未提供 macro snapshot")
    else:
        lines.append(f"- **整體 regime：{macro.regime}**")
        lines.append("")
        lines.append("| 訊號 | 狀態 |")
        lines.append("|---|---|")
        lines.append(f"| TAIEX 收盤 | {_fmt(macro.taiex_close)} |")
        lines.append(f"| TAIEX 趨勢（vs 200d SMA）| {macro.label_taiex_trend()} |")
        lines.append(f"| TAIEX 60 日報酬 | {macro.label_60d()} |")
        lines.append(f"| 廣度（中期趨勢）| {macro.label_breadth()} |")
        lines.append(f"| 外資 20 日累計 | {macro.label_foreign()} |")
        lines.append(f"| 融資餘額 YoY | {macro.label_margin()} |")
    lines.append("")

    lines.append("## Step 4–5 產業面")
    lines.append("> ⚠️ Phase 6 跳過（需手動 YAML），可後續補")
    lines.append("")

    lines.append("## Step 6 贏勢股 8 條規則")
    lines.append("")
    lines.append("| 編號 | 規則 | 結果 | 說明 |")
    lines.append("|---|---|---|---|")
    for r in sb.results:
        lines.append(_rule_row(r))
    lines.append("")
    lines.append(f"通過：{', '.join(sb.passed_codes) or '無'}")
    lines.append(f"未通過：{', '.join(sb.failed_codes) or '無'}")
    if sb.unknown_codes:
        lines.append(f"資料不足：{', '.join(sb.unknown_codes)}")
    lines.append("")

    lines.append("## Step 7 評價")
    lines.append("")
    if snapshot is None:
        lines.append("> 資料不足（缺日股價或財報）")
    else:
        s = snapshot
        lines.append(f"- 自動選用方法：**{s.method}**")
        lines.append(f"- 最新收盤：{_fmt(s.current_close)}")
        lines.append(f"- 當前 trailing {s.method}：{_fmt(s.current_multiple)}")
        if s.forward_metric is not None:
            label_map = {"PE": ("Forward EPS", "Forward PE"), "PS": ("Forward SPS", "Forward PS")}
            metric_label, multi_label = label_map.get(s.method, ("Forward metric", "Forward multiple"))
            lines.append(f"- **{metric_label}（月營收動能外推）：{_fmt(s.forward_metric)}**")
            lines.append(
                f"- **{multi_label}（close / {metric_label.split()[-1]}）：{_fmt(s.forward_multiple)}**"
            )
            lines.append(f"- 上行空間使用 {metric_label} 與歷史 trailing {s.method} band 比較（不對稱）")
        else:
            lines.append(f"- 每股指標（{_per_share_label(s.method)}）：{_fmt(s.per_share_metric)}")
        if s.band.mean is not None:
            lines.append(
                f"- 5Y {s.method} 區間：mean={_fmt(s.band.mean)}，"
                f"−1σ={_fmt(s.band.minus_1sd)}，+1σ={_fmt(s.band.plus_1sd)}"
                f"（n={s.band.n_obs}）"
            )
            lines.append(f"- 目前所在區間：{s.band_position or '—'}")
            lines.append(
                f"- 目標價：mean={_fmt(s.target_mean)}（潛在 {_fmt(s.upside_mean, pct=True)}），"
                f"−1σ={_fmt(s.target_minus_1sd)}（{_fmt(s.upside_lower, pct=True)}），"
                f"+1σ={_fmt(s.target_plus_1sd)}（{_fmt(s.upside_upper, pct=True)}）"
            )
        else:
            lines.append("- 5Y 區間：資料不足")
        lines.append(f"- **決策：{s.decision}**")
    lines.append("")

    lines.append("## 附錄：核心指標數值")
    lines.append("")
    lines.append("| 指標 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| TTM 營收 | {_fmt(revenue.ttm_revenue(qs))} |")
    lines.append(f"| TTM 營收 YoY | {_fmt(revenue.revenue_yoy_ttm(qs), pct=True)} |")
    lines.append(f"| TTM 淨利 | {_fmt(profitability.ttm_net_income(qs))} |")
    lines.append(f"| TTM 淨利 YoY | {_fmt(profitability.net_income_yoy_ttm(qs), pct=True)} |")
    if qs:
        latest = qs[-1]
        gm = (
            latest.gross_profit / latest.revenue if latest.gross_profit and latest.revenue else None
        )
        lines.append(f"| 最新單季毛利率 | {_fmt(gm, pct=True)} |")
        lines.append(
            f"| 最新單季營業利益率 | {_fmt(profitability.operating_margin(latest), pct=True)} |"
        )
        lines.append(f"| 最新單季存貨天數 | {_fmt(efficiency.inventory_days(latest))} |")
        lines.append(f"| 最新單季負債比 | {_fmt(leverage.debt_ratio(latest), pct=True)} |")
    lines.append(f"| FCF 連續為負年數 | {cashflow.fcf_consecutive_negative_years(qs)} |")
    lines.append(f"| EPS_TTM | {_fmt(per_share.eps_ttm(qs))} |")
    if qs:
        lines.append(f"| BVPS（最新季） | {_fmt(per_share.bvps(qs[-1]))} |")
    lines.append(f"| SPS_TTM | {_fmt(per_share.sps_ttm(qs))} |")

    lines.append("")
    lines.append("### 最近 8 季原始資料")
    lines.append("")
    if qs:
        lines.append(
            "| period | revenue | gross_profit | op_income | net_income | eps | OCF | FCF |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        for q in qs[-8:]:
            lines.append(
                f"| {q.period} "
                f"| {_fmt(q.revenue)} "
                f"| {_fmt(q.gross_profit)} "
                f"| {_fmt(q.operating_income)} "
                f"| {_fmt(q.net_income)} "
                f"| {_fmt(q.eps)} "
                f"| {_fmt(q.operating_cash_flow)} "
                f"| {_fmt(q.free_cash_flow)} |"
            )
    return "\n".join(lines)


def _per_share_label(method: str) -> str:
    return {"PE": "EPS_TTM", "PB": "BVPS", "PS": "SPS_TTM"}.get(method, "—")


# Backwards-compat alias for the Phase 1 entry point.
def render_phase1_report(
    ticker: str,
    meta: StockMeta | None,
    quarterly: list[QuarterlyReport],
    monthly: list[MonthlyRevenue],
    as_of_label: str,
) -> str:
    return render_phase2_report(ticker, meta, quarterly, monthly, [], as_of_label)
