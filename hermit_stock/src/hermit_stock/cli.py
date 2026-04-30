"""hermit-stock CLI entry point."""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import typer

# Windows defaults stdout to cp950 which chokes on the tick/cross marks the
# reports use. Python 3.7+ supports reconfiguring on the fly.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from .data.adapters import db_adapter
from .reports.analyzer import render_phase2_report
from .reports.screener import screen_from_db
from .reports.screener_io import to_excel, to_markdown
from .scrapers import capital_reductions, dividends

app = typer.Typer(add_completion=False, help="Taiwan winning-stock screener")


@app.command()
def analyze(
    ticker: str = typer.Argument(..., help="Stock ticker, e.g. 2330"),
    as_of: str = typer.Option(
        ..., "--as-of", help="Cut-off period (e.g. 2024Q3) or ISO date (2024-11-14)"
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Write report to file (default: print to stdout)"
    ),
    forward_eps: bool = typer.Option(
        False,
        "--forward-eps",
        help="Compute forward PE from monthly-revenue YoY momentum and use it for upside",
    ),
) -> None:
    """Phase 2 analyzer: indicators + scoring + valuation, lookahead-bias safe."""
    from datetime import date as _date

    from .macro import loaders as macro_loaders
    from .macro.regime import snapshot_at

    meta = db_adapter.load_stock_meta(ticker)
    quarterly = db_adapter.load_quarterly_reports(ticker)
    monthly = db_adapter.load_monthly_revenue(ticker)
    prices = db_adapter.load_daily_prices(ticker)

    # Macro snapshot at as_of
    if "Q" in as_of:
        from .data.publish_date import parse_period_label, quarter_publish_date

        y, q = parse_period_label(as_of)
        as_of_d = quarter_publish_date(y, q)
    else:
        as_of_d = _date.fromisoformat(as_of)
    taiex = macro_loaders.load_taiex(end=as_of_d)
    breadth = macro_loaders.load_market_breadth_trend(end=as_of_d)
    fnet = macro_loaders.load_foreign_net_aggregated(end=as_of_d)
    margin = macro_loaders.load_margin_balance(end=as_of_d)
    macro_snap = snapshot_at(
        as_of_d, taiex=taiex, breadth_df=breadth, foreign_daily=fnet, margin=margin
    )
    report = render_phase2_report(
        ticker,
        meta,
        quarterly,
        monthly,
        prices,
        as_of,
        macro=macro_snap,
        use_forward_eps=forward_eps,
    )
    if output:
        output.write_text(report, encoding="utf-8")
        typer.echo(f"wrote {output}")
    else:
        typer.echo(report)


@app.command()
def screen(
    as_of: str = typer.Option(
        ..., "--as-of", help="Cut-off period (e.g. 2024Q3) or ISO date (2024-11-14)"
    ),
    output: Path = typer.Option(Path("screener.xlsx"), "--output", "-o", help="Excel output path"),
    min_score: int = typer.Option(
        0, "--min-score", help="Only include rows with total score >= this value"
    ),
    min_valuation_score: int = typer.Option(
        5,
        "--min-valuation-score",
        help="Only run valuation (PE/PB/PS, 5y band) for rows with total score >= this. "
        "Speeds up the screener by skipping per-ticker daily-price loads for low-scoring rows.",
    ),
    tickers: str | None = typer.Option(
        None, "--tickers", help="Comma-separated tickers (default: all TWSE+TPEx STOCK)"
    ),
    no_valuation: bool = typer.Option(
        False, "--no-valuation", help="Skip Stage 2 valuation entirely (much faster)"
    ),
    markdown: bool = typer.Option(
        False, "--markdown", help="Also print top 50 as markdown to stdout"
    ),
) -> None:
    """Phase 3 screener: rank the universe by 8-rule score + valuation snapshot."""
    ticker_list = [t.strip() for t in tickers.split(",")] if tickers else None
    rows = screen_from_db(
        as_of_label=as_of,
        tickers=ticker_list,
        with_valuation=not no_valuation,
        min_valuation_score=min_valuation_score,
    )
    rows = [r for r in rows if r.scoreboard.score >= min_score]
    to_excel(rows, output)
    typer.echo(f"wrote {output} ({len(rows)} rows)")
    if markdown:
        typer.echo(to_markdown(rows, top_n=50))


@app.command()
def backtest(
    start: str = typer.Option("2017-01-01", "--start", help="ISO date"),
    end: str = typer.Option("2024-09-30", "--end", help="ISO date"),
    top_k: int = typer.Option(10, "--top-k"),
    min_score_floor: int = typer.Option(3, "--min-score-floor"),
    initial_cash: float = typer.Option(1_000_000.0, "--initial-cash"),
    output_dir: Path = typer.Option(Path("backtest_out"), "--output", "-o"),
    ablation: bool = typer.Option(
        False, "--ablation", help="Also run 16 rule-ablation variants (drop_F_i + only_F_i)"
    ),
    gate_rules: str = typer.Option(
        "F6,F7,F8",
        "--gate-rules",
        help="Comma-separated rule codes that must all pass (excluded from score). "
        "Default 'F6,F7,F8' is the empirical best (variant (b) in tuning).",
    ),
    f3_years: int = typer.Option(
        2,
        "--f3-years",
        help="F3 threshold: required consecutive years of rising gross margin (default 2)",
    ),
    macro_filter: bool = typer.Option(
        False,
        "--macro-filter",
        help="In Bear regime, halve top_k (concentrate into highest-conviction names)",
    ),
    elite_override: bool = typer.Option(
        False,
        "--elite-override",
        help="Rescue F7/F8-only gate fails when score>=7 + elite quality "
        "(gross margin >=40%, op margin >=25%, ROE >=20%, or FCF margin >=20%).",
    ),
    min_avg_turnover: float = typer.Option(
        0.0,
        "--min-avg-turnover",
        help="Liquidity filter: minimum 60-day rolling-mean turnover (NTD) at rebalance day. "
        "0 disables. Practical floor: 50_000_000 (5000 萬) for mid-caps; "
        "200_000_000 (2 億) for larger institutional friendly.",
    ),
    include_delisted: bool = typer.Option(
        False,
        "--include-delisted",
        help="Include delisted stocks in universe (eliminates survivorship bias). "
        "Slower but yields a more honest backtest result.",
    ),
    benchmark: str = typer.Option("TAIEX", "--benchmark"),
) -> None:
    """Run the strategy backtest, write summary + PNG + CSV to --output."""
    from .backtest.ablation import run_ablation_suite
    from .backtest.engine import BacktestConfig, build_adj_close_table
    from .backtest.engine import run_backtest as _rb
    from .backtest.metrics import compute_metrics
    from .reports.backtest_report import (
        ablation_summary_md,
        plot_ablation_comparison,
        plot_drawdown,
        plot_nav,
        render_summary_md,
        write_holdings_csv,
        write_trades_csv,
    )
    from .scoring.rules import Thresholds

    output_dir.mkdir(parents=True, exist_ok=True)
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)

    typer.echo(f"[backtest] loading universe (include_delisted={include_delisted}) ...")
    metas = db_adapter.load_active_stocks(include_delisted=include_delisted)
    ticker_list = [m.ticker for m in metas]
    typer.echo(f"[backtest] universe size: {len(ticker_list)}")

    typer.echo(
        f"[backtest] loading quarterly + monthly + prices for {len(ticker_list)} tickers ..."
    )
    quarterly = db_adapter.load_all_quarterly_reports(ticker_list)
    monthly = db_adapter.load_all_monthly_revenue(ticker_list)
    prices = db_adapter.load_all_daily_prices(ticker_list)
    dividends_data = db_adapter.load_all_dividends(ticker_list)
    reductions_data = db_adapter.load_all_capital_reductions(ticker_list)
    typer.echo(
        f"[backtest] loaded: financials={len(quarterly)}, monthly={len(monthly)}, "
        f"prices={len(prices)}, dividends={len(dividends_data)}, reductions={len(reductions_data)}"
    )

    typer.echo("[backtest] computing adjusted close ...")
    adj_close = build_adj_close_table(quarterly, prices, dividends_data, reductions_data)
    typer.echo(f"[backtest] adj_close shape: {adj_close.shape}")

    gate_set = frozenset(g.strip() for g in gate_rules.split(",") if g.strip())
    thresholds = Thresholds(gross_margin_consecutive_years=f3_years) if f3_years != 2 else None
    label_parts = []
    if gate_set:
        label_parts.append("gate_" + "_".join(sorted(gate_set)))
    if f3_years != 2:
        label_parts.append(f"F3y{f3_years}")
    label = "main_" + "_".join(label_parts) if label_parts else "main"
    if macro_filter:
        label = label + "_macro"
    if include_delisted:
        label = label + "_full"
    if min_avg_turnover > 0:
        label = label + f"_liq{int(min_avg_turnover/1e6)}M"
    if elite_override:
        label = label + "_elite"
    cfg = BacktestConfig(
        start=start_d,
        end=end_d,
        top_k=top_k,
        min_score_floor=min_score_floor,
        initial_cash=initial_cash,
        gate_rules=gate_set,
        thresholds=thresholds,
        macro_filter=macro_filter,
        min_avg_turnover=min_avg_turnover,
        elite_override=elite_override,
        label=label,
    )
    typer.echo(f"[backtest] main run: {start_d} → {end_d}, top_k={top_k}, floor={min_score_floor}")

    regime_ts: pd.Series | None = None
    if macro_filter:
        from .macro import loaders as macro_loaders
        from .macro.regime import regime_series

        typer.echo("[backtest] loading macro inputs for regime classification ...")
        taiex = macro_loaders.load_taiex(start=start_d, end=end_d)
        breadth_df = macro_loaders.load_market_breadth_trend(start=start_d, end=end_d)
        fnet = macro_loaders.load_foreign_net_aggregated(start=start_d, end=end_d)
        regime_ts = regime_series(taiex=taiex, breadth_df=breadth_df, foreign_daily=fnet)
        if not regime_ts.empty:
            counts = regime_ts.value_counts().to_dict()
            typer.echo(f"[backtest] regime distribution: {counts}")

    turnover_60d: pd.DataFrame | None = None
    if min_avg_turnover > 0:
        typer.echo("[backtest] loading turnover for liquidity filter ...")
        raw_turnover = db_adapter.load_turnover_table(ticker_list)
        # 60-trading-day rolling mean, computed once on the wide table
        turnover_60d = raw_turnover.rolling(window=60, min_periods=20).mean()
        typer.echo(f"[backtest] turnover_60d shape: {turnover_60d.shape}")

    main = _rb(
        cfg,
        metas=metas,
        quarterly_by_ticker=quarterly,
        monthly_by_ticker=monthly,
        adj_close=adj_close,
        regime_series=regime_ts,
        turnover_60d=turnover_60d,
    )

    bench_pairs = db_adapter.load_index_close(benchmark)
    bench_series = pd.Series({d: c for d, c in bench_pairs})
    bench_series.index = pd.to_datetime(bench_series.index)
    metrics = compute_metrics(main.nav, bench_series)

    summary_path = output_dir / "summary.md"
    summary_path.write_text(
        render_summary_md(main, metrics, benchmark_label=benchmark), encoding="utf-8"
    )
    plot_nav(main, bench_series, output_dir / "nav.png", benchmark_label=benchmark)
    plot_drawdown(main, output_dir / "drawdown.png")
    write_holdings_csv(main, output_dir / "holdings.csv")
    write_trades_csv(main, output_dir / "trades.csv")
    typer.echo(
        f"[backtest] main: cumret={metrics.cumulative_return*100:.2f}%, "
        f"ann={metrics.annual_return*100:.2f}%, sharpe={metrics.sharpe:.2f}, "
        f"mdd={metrics.max_drawdown*100:.2f}%"
    )

    if ablation:
        typer.echo("[backtest] running 16 ablations ...")
        results = run_ablation_suite(
            cfg,
            metas=metas,
            quarterly_by_ticker=quarterly,
            monthly_by_ticker=monthly,
            adj_close=adj_close,
        )
        plot_ablation_comparison(results, output_dir / "ablation.png")
        (output_dir / "ablation_summary.md").write_text(
            ablation_summary_md(results, bench_series), encoding="utf-8"
        )
        typer.echo(f"[backtest] ablation done, {len(results)} variants")

    typer.echo(f"[backtest] outputs in {output_dir}/")


@app.command("backfill-dividends")
def backfill_dividends_cmd(
    tickers: str | None = typer.Option(
        None, "--tickers", help="Comma-separated tickers (default: all TWSE+TPEx STOCK)"
    ),
    interval: float = typer.Option(
        0.4, "--interval", help="Seconds between FinMind requests (default 0.4 ≈ 2.5 req/s)"
    ),
    skip_reductions: bool = typer.Option(
        False, "--skip-reductions", help="Only backfill dividends, not capital reductions"
    ),
    skip_existing: bool = typer.Option(
        True,
        "--skip-existing/--no-skip-existing",
        help="Skip tickers already present in DB (resumable mode)",
    ),
) -> None:
    """Populate tw.dividends and tw.capital_changes from FinMind.

    Run once before backtesting. ~30 minutes for the full 1948-ticker universe.
    Re-running is idempotent (DELETE + INSERT per ticker).
    """
    if tickers:
        ticker_list = [t.strip() for t in tickers.split(",")]
    else:
        metas = db_adapter.load_active_stocks()
        ticker_list = [m.ticker for m in metas]

    if skip_existing:
        existing_div = set(db_adapter.load_all_dividends().keys())
        before = len(ticker_list)
        div_targets = [t for t in ticker_list if t not in existing_div]
        typer.echo(
            f"[backfill] universe size = {before}, "
            f"existing dividends in DB = {len(existing_div)}, "
            f"to fetch = {len(div_targets)}"
        )
    else:
        div_targets = ticker_list
        typer.echo(f"[backfill] universe size = {len(ticker_list)} (force-refetch)")

    def cb(done: int, total: int, t: str, error: str | None) -> None:
        if error:
            typer.echo(f"  [{done}/{total}] {t}: ERROR {error}")
        else:
            typer.echo(f"  [{done}/{total}] {t}")

    db_adapter._load_env_once()
    token = os.getenv("FINMIND_TOKEN") or None
    if token:
        typer.echo(f"[backfill] using FinMind token (len={len(token)})")
    else:
        typer.echo("[backfill] no FINMIND_TOKEN; running anonymous (lower quota)")

    typer.echo(f"[backfill] dividends: {len(div_targets)} tickers ...")
    div_summary = dividends.backfill_dividends(
        div_targets, interval=interval, on_progress=cb, token=token
    )
    div_total = sum(v for v in div_summary.values() if v > 0)
    div_errors = sum(1 for v in div_summary.values() if v == -1)
    typer.echo(f"[backfill] dividends: {div_total} rows, {div_errors} ticker errors")

    if not skip_reductions:
        if skip_existing:
            existing_red = set(db_adapter.load_all_capital_reductions().keys())
            # for reductions we have to fetch every ticker since most return 0 rows;
            # "existing" only catches tickers that DID have a reduction. Reduction
            # is rare, so we always fetch all unless the user disables.
            red_targets = ticker_list  # always fetch all (cheap to confirm 0 rows)
            typer.echo(
                f"[backfill] reductions: existing={len(existing_red)}, "
                f"fetching all {len(red_targets)} (reduction events are rare)"
            )
        else:
            red_targets = ticker_list
        red_summary = capital_reductions.backfill_reductions(
            red_targets, interval=interval, on_progress=cb, token=token
        )
        red_total = sum(v for v in red_summary.values() if v > 0)
        red_errors = sum(1 for v in red_summary.values() if v == -1)
        typer.echo(f"[backfill] reductions: {red_total} rows, {red_errors} ticker errors")


@app.command()
def version() -> None:
    from . import __version__

    typer.echo(__version__)


if __name__ == "__main__":
    app()
