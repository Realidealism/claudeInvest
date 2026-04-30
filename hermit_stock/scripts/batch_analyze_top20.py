"""Generate analyze reports for the current Top-20 picks.

Shares data loads across tickers to avoid 20x DB roundtrips.
Output: one .md file per ticker under top20_reports/.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from hermit_stock.backtest.engine import select_top_k
from hermit_stock.data.adapters import db_adapter
from hermit_stock.data.models import StockMeta
from hermit_stock.macro import loaders as macro_loaders
from hermit_stock.macro.regime import snapshot_at
from hermit_stock.reports.analyzer import render_phase2_report

AS_OF = date(2026, 4, 30)
AS_OF_LABEL = "2026-04-30"
OUT_DIR = Path("top20_reports_fwd")
USE_FORWARD_EPS = True


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    print("loading universe ...")
    metas: list[StockMeta] = db_adapter.load_active_stocks()
    by_ticker = {m.ticker: m for m in metas}
    tl = [m.ticker for m in metas]

    quarterly = db_adapter.load_all_quarterly_reports(tl)
    monthly = db_adapter.load_all_monthly_revenue(tl)

    print("computing Top-20 picks ...")
    picks = select_top_k(
        AS_OF,
        metas,
        quarterly,
        monthly,
        enabled_rules=frozenset({"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"}),
        gate_rules=frozenset({"F6", "F7", "F8"}),
        top_k=20,
        min_score_floor=3,
    )
    target_tickers = [t for t, _, _ in picks]
    print(f"got {len(target_tickers)} picks")

    print("loading prices for picks ...")
    prices_by_ticker = db_adapter.load_all_daily_prices(target_tickers)

    print("loading macro inputs ...")
    taiex = macro_loaders.load_taiex(end=AS_OF)
    breadth = macro_loaders.load_market_breadth_trend(end=AS_OF)
    fnet = macro_loaders.load_foreign_net_aggregated(end=AS_OF)
    margin = macro_loaders.load_margin_balance(end=AS_OF)
    macro_snap = snapshot_at(
        AS_OF, taiex=taiex, breadth_df=breadth, foreign_daily=fnet, margin=margin
    )

    index_md_lines: list[str] = []
    index_md_lines.append(f"# Top 20 picks — as_of {AS_OF_LABEL}")
    index_md_lines.append("")
    index_md_lines.append(f"- Macro regime: **{macro_snap.regime}**")
    index_md_lines.append("- Strategy: gate F6+F7+F8, floor=3, Top-20 ranked by full 8-rule score")
    index_md_lines.append("")
    index_md_lines.append("| # | Ticker | Name | Score | Industry | Report |")
    index_md_lines.append("|---|---|---|---|---|---|")

    for i, (ticker, score_full, _results) in enumerate(picks, start=1):
        meta = by_ticker.get(ticker)
        name = meta.name if meta else ticker
        industry = (meta.industry if meta else "") or ""
        prices = prices_by_ticker.get(ticker, [])
        report_md = render_phase2_report(
            ticker,
            meta,
            quarterly.get(ticker, []),
            monthly.get(ticker, []),
            prices,
            AS_OF_LABEL,
            macro=macro_snap,
            use_forward_eps=USE_FORWARD_EPS,
        )
        out_path = OUT_DIR / f"{i:02d}_{ticker}_{name}.md"
        out_path.write_text(report_md, encoding="utf-8")
        index_md_lines.append(
            f"| {i} | {ticker} | {name} | {score_full} | {industry} | "
            f"[{out_path.name}]({out_path.name}) |"
        )
        print(f"  [{i:>2}/20] {ticker} {name} ({score_full}/8)")

    (OUT_DIR / "INDEX.md").write_text("\n".join(index_md_lines), encoding="utf-8")
    print(f"\nwrote {len(picks)} reports + INDEX.md to {OUT_DIR}/")


if __name__ == "__main__":
    main()
