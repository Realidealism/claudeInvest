"""
Financial statement charts — 財報視覺化.

Builds Plotly figures for:
  - pe_river_chart   本益比河流圖 (price overlaid with historical PE percentile bands)
  - trend_chart      Generic multi-line trend for quarterly metrics
  - dupont_chart     ROE DuPont decomposition (stacked contribution)

Usage:
  python -m charts.financials 2330           # exports <stock_id>_financials.html
  python -m charts.financials 2330 --show    # opens in default browser
"""

from __future__ import annotations

import webbrowser
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from analysis.financials.profitability import get_profitability, get_dupont
from analysis.financials.safety import get_safety
from analysis.financials.growth import get_growth
from analysis.financials.cashflow_quality import get_cashflow_analysis
from analysis.financials.valuation import get_pe_history, get_pe_bands
from db.connection import get_cursor


# Band fill colors (from cheap → expensive)
BAND_COLORS = [
    "rgba(46, 204, 113, 0.25)",   # <p10  便宜
    "rgba(52, 152, 219, 0.20)",   # p10-p25
    "rgba(241, 196, 15, 0.18)",   # p25-p50
    "rgba(230, 126, 34, 0.18)",   # p50-p75 偏貴
    "rgba(231, 76, 60, 0.22)",    # >p75
]
BAND_LABELS = ["便宜 (<p10)", "p10-p25", "p25-p50", "p50-p75", "偏貴 (>p75)"]


def _period_labels(rows: list[dict]) -> list[str]:
    return [f"{r['year']}Q{r['quarter']}" for r in rows]


# ---------- PE River Chart ----------

def pe_river_chart(stock_id: str, years: int = 10) -> go.Figure:
    """
    Build the classic PE band (river) chart.
    Each band line = percentile_PE × TTM_EPS_at_date. As EPS grows over time,
    the bands drift upward, forming a "river" of historical valuation.
    """
    history = get_pe_history(stock_id, years)
    band_info = get_pe_bands(stock_id, years)

    if not history or band_info.get("status") != "ok":
        fig = go.Figure()
        fig.add_annotation(text=f"{stock_id} — insufficient PE history",
                           xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    bands = band_info["pe_bands"]
    dates = [h["date"] for h in history]
    closes = [h["close"] for h in history]
    ttm = [h["ttm_eps"] for h in history]

    # Each band line: band_pe × ttm_eps at that date
    band_series = {p: [pe_mul * e for e in ttm] for p, pe_mul in bands.items()}

    fig = make_subplots(rows=1, cols=1)

    # Fill between adjacent bands
    percentiles = sorted(bands.keys())   # [10, 25, 50, 75, 90]
    # Bottom fill (below lowest band)
    fig.add_trace(go.Scatter(
        x=dates, y=[0] * len(dates), mode="lines",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    prev_series = None
    for i, p in enumerate(percentiles):
        series = band_series[p]
        fig.add_trace(go.Scatter(
            x=dates, y=series, mode="lines",
            line=dict(color="rgba(100,100,100,0.6)", width=1, dash="dash"),
            name=f"PE={bands[p]:.1f} (p{p})",
            fill="tonexty" if prev_series is not None else None,
            fillcolor=BAND_COLORS[i] if prev_series is not None else None,
        ))
        prev_series = series

    # Price line on top
    fig.add_trace(go.Scatter(
        x=dates, y=closes, mode="lines", name="收盤價",
        line=dict(color="#111", width=2),
    ))

    current_percentile = band_info["current_percentile"]
    current_pe = band_info["current_pe"]
    fig.update_layout(
        title=(f"{stock_id}  本益比河流圖 ({years} 年)"
               f"  |  當前 PE={current_pe}  percentile={current_percentile}%"),
        xaxis_title="日期",
        yaxis_title="股價 (NTD)",
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", y=-0.15),
    )
    return fig


# ---------- Generic trend chart ----------

def trend_chart(rows: list[dict], fields: list[tuple[str, str]],
                title: str, y_suffix: str = "%") -> go.Figure:
    """
    Plot multiple quarterly metrics as lines.
    fields: list of (field_key, display_name).
    """
    labels = _period_labels(rows)
    fig = go.Figure()
    for key, name in fields:
        y = [r.get(key) for r in rows]
        fig.add_trace(go.Scatter(x=labels, y=y, mode="lines+markers", name=name))
    fig.update_layout(
        title=title,
        xaxis_title="季度",
        yaxis_title=f"({y_suffix})" if y_suffix else "",
        hovermode="x unified",
        template="plotly_white",
    )
    return fig


def dupont_chart(stock_id: str, quarters: int = 20) -> go.Figure:
    """
    DuPont visualization: three bars (NM, AT, EM) + ROE line on secondary axis.
    """
    rows = get_dupont(stock_id, quarters)
    labels = _period_labels(rows)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=labels, y=[r["net_margin"] for r in rows],
                         name="淨利率 %", marker_color="#3498db"), secondary_y=False)
    fig.add_trace(go.Scatter(x=labels, y=[r["asset_turnover"] for r in rows],
                             mode="lines+markers", name="資產週轉率",
                             line=dict(color="#27ae60")), secondary_y=True)
    fig.add_trace(go.Scatter(x=labels, y=[r["equity_multiplier"] for r in rows],
                             mode="lines+markers", name="權益乘數",
                             line=dict(color="#e67e22")), secondary_y=True)
    fig.add_trace(go.Scatter(x=labels, y=[r["roe_decomposed"] for r in rows],
                             mode="lines+markers", name="ROE %",
                             line=dict(color="#c0392b", width=3)), secondary_y=False)

    fig.update_layout(
        title=f"{stock_id}  杜邦分析 (ROE 拆解)",
        hovermode="x unified",
        template="plotly_white",
    )
    fig.update_yaxes(title_text="% (淨利率, ROE)", secondary_y=False)
    fig.update_yaxes(title_text="倍數 (週轉率, 權益乘數)", secondary_y=True)
    return fig


# ---------- Report generator ----------

def build_report(stock_id: str, quarters: int = 20, years: int = 10) -> list[go.Figure]:
    """Build all charts for a single stock."""
    prof = get_profitability(stock_id, quarters)
    safe = get_safety(stock_id, quarters)
    grow = get_growth(stock_id, quarters)
    cfq = get_cashflow_analysis(stock_id, quarters)

    figs = [
        pe_river_chart(stock_id, years),
        trend_chart(prof, [
            ("gross_margin", "毛利率"),
            ("operating_margin", "營業利益率"),
            ("net_margin", "稅後淨利率"),
        ], f"{stock_id}  獲利能力"),
        trend_chart(prof, [
            ("roe_annualized", "ROE (年化)"),
            ("roa_annualized", "ROA (年化)"),
        ], f"{stock_id}  ROE / ROA"),
        dupont_chart(stock_id, quarters),
        trend_chart(grow, [
            ("revenue_yoy", "營收 YoY"),
            ("gross_profit_yoy", "毛利 YoY"),
            ("operating_income_yoy", "營業利益 YoY"),
            ("net_income_yoy", "稅後淨利 YoY"),
            ("eps_yoy", "EPS YoY"),
        ], f"{stock_id}  成長率 (YoY)"),
        trend_chart(safe, [
            ("current_ratio", "流動比率"),
            ("quick_ratio", "速動比率"),
            ("debt_ratio", "負債比率"),
        ], f"{stock_id}  安全性"),
        trend_chart(cfq, [
            ("ocf_to_ni", "OCF / 淨利"),
            ("fcf_margin", "FCF / 營收 %"),
            ("capex_intensity", "CAPEX / 營收 %"),
        ], f"{stock_id}  現金流品質", y_suffix=""),
        trend_chart(cfq, [
            ("dso", "應收天數 DSO"),
            ("dio", "存貨天數 DIO"),
            ("dpo", "應付天數 DPO"),
            ("ccc", "現金轉換循環 CCC"),
        ], f"{stock_id}  營運效率 (天)", y_suffix="天"),
    ]
    return figs


def save_html(stock_id: str, path: str | None = None, show: bool = False) -> str:
    """Render all charts into one self-contained HTML file."""
    figs = build_report(stock_id)

    # Fetch stock name for title
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT name FROM tw.stocks WHERE stock_id = %s", (stock_id,))
        r = cur.fetchone()
        name = r["name"] if r else ""

    out_path = Path(path) if path else Path(f"{stock_id}_financials.html")

    html_parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{stock_id} {name} 財報分析</title>",
        "<style>body{font-family:sans-serif;max-width:1400px;margin:20px auto;"
        "padding:0 20px}h1{border-bottom:2px solid #333;padding-bottom:8px}</style>",
        "</head><body>",
        f"<h1>{stock_id} {name} — 財報分析</h1>",
    ]
    for fig in figs:
        html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
    html_parts.append("</body></html>")

    out_path.write_text("".join(html_parts), encoding="utf-8")
    print(f"Wrote {out_path.resolve()}")

    if show:
        webbrowser.open(out_path.resolve().as_uri())
    return str(out_path)


if __name__ == "__main__":
    import sys
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    show = "--show" in sys.argv
    sid = args[0] if args else "2330"
    save_html(sid, show=show)
