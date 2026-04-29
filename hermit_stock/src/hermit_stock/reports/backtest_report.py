"""Render backtest outputs: metrics summary, NAV/drawdown PNGs, holdings/trades CSVs."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from ..backtest.engine import BacktestResult  # noqa: E402
from ..backtest.metrics import Metrics, compute_metrics  # noqa: E402


def _normalize_to_one(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    return s / s.iloc[0]


def render_summary_md(
    result: BacktestResult,
    metrics: Metrics,
    *,
    benchmark_label: str = "TAIEX",
) -> str:
    cfg = result.config
    lines: list[str] = []
    lines.append(f"# 回測報告 — {cfg.label}")
    lines.append("")
    lines.append(f"- 期間：{cfg.start} → {cfg.end}")
    lines.append(f"- 初始資金：{cfg.initial_cash:,.0f} NTD")
    lines.append(f"- Top-K：{cfg.top_k}，min_score_floor：{cfg.min_score_floor}")
    lines.append(f"- 啟用規則：{', '.join(sorted(cfg.enabled_rules))}")
    lines.append(f"- Rebalance 次數：{len(result.rebalance_dates_used)}")
    lines.append(f"- Trade 筆數：{len(result.portfolio.trade_log)}")
    lines.append("")
    lines.append("## 績效")
    lines.append("")
    lines.append("| 指標 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| 累積報酬 | {metrics.cumulative_return * 100:.2f}% |")
    lines.append(f"| 年化報酬 | {metrics.annual_return * 100:.2f}% |")
    lines.append(f"| 年化波動 | {metrics.annual_volatility * 100:.2f}% |")
    lines.append(f"| Sharpe | {metrics.sharpe:.2f} |")
    lines.append(f"| 最大回撤 | {metrics.max_drawdown * 100:.2f}% |")
    lines.append(f"| 日勝率 | {metrics.win_rate_daily * 100:.2f}% |")
    if metrics.beta is not None:
        lines.append(f"| Beta vs {benchmark_label} | {metrics.beta:.2f} |")
        lines.append(f"| Alpha (annualized) | {(metrics.alpha or 0) * 100:.2f}% |")
        if metrics.benchmark_cumret is not None:
            lines.append(f"| {benchmark_label} 累積報酬 | {metrics.benchmark_cumret * 100:.2f}% |")
    return "\n".join(lines)


def plot_nav(
    result: BacktestResult,
    benchmark: pd.Series | None,
    out_path: Path,
    *,
    benchmark_label: str = "TAIEX",
) -> None:
    nav = _normalize_to_one(result.nav)
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(nav.index, nav.values, label=f"Portfolio ({result.config.label})", linewidth=1.6)  # type: ignore[arg-type]
    if benchmark is not None and not benchmark.empty:
        b = _normalize_to_one(benchmark.reindex(nav.index, method="ffill"))
        ax.plot(b.index, b.values, label=benchmark_label, linewidth=1.2, linestyle="--", alpha=0.85)  # type: ignore[arg-type]
    ax.set_title(f"NAV — {result.config.label}")
    ax.set_ylabel("Cumulative growth (×1)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_drawdown(result: BacktestResult, out_path: Path) -> None:
    nav = result.nav
    if nav.empty:
        return
    running_max = nav.cummax()
    dd = (nav / running_max - 1.0) * 100.0
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.fill_between(dd.index, dd.values, 0, color="tab:red", alpha=0.4)  # type: ignore[arg-type]
    ax.plot(dd.index, dd.values, color="tab:red", linewidth=1.0)  # type: ignore[arg-type]
    ax.set_title(f"Drawdown — {result.config.label}")
    ax.set_ylabel("Drawdown (%)")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def write_holdings_csv(result: BacktestResult, out_path: Path) -> None:
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rebalance_date", "ticker", "shares", "is_new"])
        for d, holdings in result.portfolio.holdings_log:
            for h in holdings:
                w.writerow([d.isoformat(), h.ticker, f"{h.shares:.4f}", int(h.is_new_this_period)])


def write_trades_csv(result: BacktestResult, out_path: Path) -> None:
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "ticker", "side", "shares", "price", "cash_delta"])
        for t in result.portfolio.trade_log:
            w.writerow(
                [
                    t.date.isoformat(),
                    t.ticker,
                    t.side,
                    f"{t.shares:.4f}",
                    f"{t.price:.4f}",
                    f"{t.cash_delta:.2f}",
                ]
            )


def plot_ablation_comparison(
    results: dict[str, BacktestResult],
    out_path: Path,
    *,
    main_label: str = "main",
) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    # Plot main first in a dark color, ablations in faded colors
    for label, r in results.items():
        if r.nav.empty:
            continue
        normed = _normalize_to_one(r.nav)
        idx = normed.index
        vals = normed.values
        if label == main_label:
            ax.plot(idx, vals, label=label, color="black", linewidth=2.0, zorder=10)  # type: ignore[arg-type]
        elif label.startswith("drop_"):
            ax.plot(idx, vals, label=label, linewidth=0.9, alpha=0.75)  # type: ignore[arg-type]
        elif label.startswith("only_"):
            ax.plot(idx, vals, label=label, linewidth=0.9, alpha=0.55, linestyle="--")  # type: ignore[arg-type]
    ax.set_title("Rule ablation — drop_F_i (solid) vs only_F_i (dashed)")
    ax.set_ylabel("Cumulative growth (×1)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=7, ncol=2)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


def ablation_summary_md(
    results: dict[str, BacktestResult],
    benchmark: pd.Series | None,
    *,
    main_label: str = "main",
) -> str:
    lines: list[str] = []
    lines.append("# Ablation summary")
    lines.append("")
    lines.append("| variant | cum_return | ann_return | sharpe | mdd | alpha | beta |")
    lines.append("|---|---|---|---|---|---|---|")
    main = results.get(main_label)
    main_metrics: Metrics | None = compute_metrics(main.nav, benchmark) if main else None

    def row(label: str, m: Metrics) -> str:
        a = f"{(m.alpha or 0) * 100:.2f}%" if m.alpha is not None else "—"
        b = f"{m.beta:.2f}" if m.beta is not None else "—"
        return (
            f"| {label} | {m.cumulative_return*100:.2f}% | {m.annual_return*100:.2f}% | "
            f"{m.sharpe:.2f} | {m.max_drawdown*100:.2f}% | {a} | {b} |"
        )

    if main_metrics is not None:
        lines.append(row(main_label, main_metrics))
    for label, r in results.items():
        if label == main_label:
            continue
        m = compute_metrics(r.nav, benchmark)
        lines.append(row(label, m))
    return "\n".join(lines)
