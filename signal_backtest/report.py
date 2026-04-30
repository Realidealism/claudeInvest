"""
Chinese text report formatting for AggregateReport.
"""

from __future__ import annotations

import pandas as pd

from signal_backtest.aggregate import AggregateReport


def _pct(x: float) -> str:
    return f"{x * 100:+.2f}%"


def _fmt_side_stats(report: AggregateReport) -> str:
    """Side-by-side comparison table for 做多 vs 做空."""
    sides = ["做多", "做空"]
    available = [s for s in sides if s in report.side_stats]
    if not available:
        return "（無交易）"

    rows = [
        ("總交易數",   lambda s: f"{s.總交易數:,}"),
        ("勝率",       lambda s: f"{s.勝率 * 100:.2f}%"),
        ("平均報酬",   lambda s: _pct(s.平均報酬)),
        ("25%",        lambda s: _pct(s.較佳25)),
        ("50%",        lambda s: _pct(s.中位數報酬)),
        ("75%",        lambda s: _pct(s.較差25)),
    ]

    header = "  指標         | " + " | ".join(f"{s:>10}" for s in available)
    sep = "-" * len(header)
    lines = [header, sep]
    for label, fn in rows:
        cells = " | ".join(f"{fn(report.side_stats[s]):>10}" for s in available)
        lines.append(f"  {label:<10} | {cells}")
    return "\n".join(lines)


def _fmt_ranking(df: pd.DataFrame, title: str) -> str:
    if df.empty:
        return f"\n【{title}】\n  （無資料）"
    cols = ["股票代號", "股票名稱", "交易數", "勝率", "平均報酬", "累計報酬", "最大回撤"]
    out = [f"\n【{title}】"]
    out.append("  " + " | ".join(f"{c:>8}" for c in cols))
    out.append("  " + "-" * (10 * len(cols)))
    for _, r in df.iterrows():
        cells = [
            f"{r['股票代號']:>8}",
            f"{r['股票名稱']:>8}",
            f"{int(r['交易數']):>8}",
            f"{r['勝率']*100:>7.1f}%",
            f"{_pct(r['平均報酬']):>8}",
            f"{_pct(r['累計報酬']):>8}",
            f"{_pct(r['最大回撤']):>8}",
        ]
        out.append("  " + " | ".join(cells))
    return "\n".join(out)


def _fmt_worst_trade(trade, title: str) -> str:
    """Render a single trade (the worst-loss case) with full defense trajectory."""
    if trade is None:
        return ""
    lines = [f"\n【{title}】"]
    lines.append(f"  {trade['股票代號']} {trade['股票名稱']}")
    lines.append(f"  進場：{trade['進場日期']} @ {trade['進場價']:.2f}")
    lines.append(
        f"  出場：{trade['出場日期']} @ {trade['出場價']:.2f}  ({trade['出場原因']})"
    )
    lines.append(
        f"  持倉：{int(trade['持倉天數'])} 天   報酬：{trade['報酬率']*100:+.2f}%"
    )
    events = trade["防守價變化"]
    if events is not None and len(events) > 0:
        lines.append(f"  防守價變化（{len(events)} 筆）:")
        for ev in events:
            price = ev["防守價"]
            if price is None or (isinstance(price, float) and price != price):
                price_str = "    N/A"
            else:
                price_str = f"{price:>8.2f}"
            lines.append(f"    {ev['日期']}  {ev['原因']:<10}  @ {price_str}")
    return "\n".join(lines)


def format_report(
    report: AggregateReport,
    signal_name: str,
    research: bool = False,
) -> str:
    """Render the full report as a single Chinese-language string."""
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(f"訊號回測報告：{signal_name}")
    lines.append("=" * 72)

    lines.append("\n■ 多空對比")
    lines.append(_fmt_side_stats(report))

    n_stocks = report.per_stock["股票代號"].nunique() if not report.per_stock.empty else 0
    if n_stocks > 1:
        lines.append(f"\n■ 涵蓋股票數：{n_stocks}")
        lines.append(_fmt_ranking(report.top_long,    "做多累計報酬 Top"))
        lines.append(_fmt_ranking(report.bottom_long, "做多累計報酬 Bottom"))
        lines.append(_fmt_ranking(report.top_short,   "做空累計報酬 Top"))
        lines.append(_fmt_ranking(report.bottom_short,"做空累計報酬 Bottom"))

    if research:
        lines.append("\n■ 研究模式：最慘交易")
        lines.append(_fmt_worst_trade(report.worst_long, "做多最慘"))
        lines.append(_fmt_worst_trade(report.worst_short, "做空最慘"))

    lines.append("")
    return "\n".join(lines)
