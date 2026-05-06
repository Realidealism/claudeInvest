"""
CLI for signal backtesting.

Usage:
    python -m signal_backtest --signal dummy
    python -m signal_backtest --signal dummy --limit 50
    python -m signal_backtest --signal dummy --output tmp/signal_backtest/dummy
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

# UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

from signal_backtest.signal import SIGNAL_FACTORIES
from signal_backtest.batch import run_batch, run_batch_multi
from signal_backtest.aggregate import aggregate
from signal_backtest.report import format_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Signal backtester (multi-stock)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--signal",
        help=f"Single signal factory. Options: {', '.join(SIGNAL_FACTORIES)}",
    )
    g.add_argument(
        "--signals",
        help="Comma-separated signal list — runs all together sharing one "
             "StockData instance per stock (much faster than invoking "
             "--signal N times).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of stocks (smoke testing).",
    )
    p.add_argument(
        "--stocks",
        type=str,
        default=None,
        help="Comma-separated stock IDs to backtest (overrides full universe). "
             "e.g. --stocks 2330,2317,2454",
    )
    p.add_argument(
        "--research",
        action="store_true",
        help="研究模式：顯示最慘交易（含防守價變化軌跡）等額外細節。",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: tmp/signal_backtest/<signal>).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel worker processes for --signals mode "
             "(default 1 = sequential). 4-8 recommended for full-universe runs.",
    )
    p.add_argument(
        "--cache",
        action="store_true",
        help="Pickle-cache built StockData under data/stock_cache/, keyed by "
             "DB latest trade_date. Auto-invalidates after daily_update writes "
             "new bars. Saves ~26%% per stock on cache hit.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    stock_ids = (
        [s.strip() for s in args.stocks.split(",") if s.strip()]
        if args.stocks else None
    )

    if args.signals:
        names = [s.strip() for s in args.signals.split(",") if s.strip()]
        unknown = [n for n in names if n not in SIGNAL_FACTORIES]
        if unknown:
            print(f"未知訊號：{', '.join(unknown)}", file=sys.stderr)
            print(f"可用訊號：{', '.join(SIGNAL_FACTORIES)}", file=sys.stderr)
            sys.exit(1)
        factories = {n: SIGNAL_FACTORIES[n] for n in names}
        output_dir = args.output or Path("tmp/signal_backtest")

        results = run_batch_multi(
            factories=factories,
            output_dir=output_dir,
            limit=args.limit,
            stock_ids=stock_ids,
            workers=args.workers,
            use_cache=args.cache,
        )
        for name, (trades_path, sides_path) in results.items():
            print(f"\n=== [{name}] ===")
            print(f"交易紀錄：{trades_path}")
            print(f"分組摘要：{sides_path}")
            trades_df = pd.read_parquet(trades_path)
            report = aggregate(trades_df)
            report_text = format_report(report, name, research=args.research)
            print(report_text)
            sub_dir = output_dir / name
            (sub_dir / "report.txt").write_text(report_text, encoding="utf-8")
            report.per_stock.to_parquet(sub_dir / "per_stock.parquet", index=False)
        return

    if args.signal not in SIGNAL_FACTORIES:
        print(f"未知訊號：{args.signal}", file=sys.stderr)
        print(f"可用訊號：{', '.join(SIGNAL_FACTORIES)}", file=sys.stderr)
        sys.exit(1)

    factory = SIGNAL_FACTORIES[args.signal]
    output_dir = args.output or Path("tmp/signal_backtest") / args.signal

    trades_path, sides_path = run_batch(
        factory=factory,
        output_dir=output_dir,
        limit=args.limit,
        stock_ids=stock_ids,
    )

    print(f"\n交易紀錄：{trades_path}")
    print(f"分組摘要：{sides_path}")

    trades_df = pd.read_parquet(trades_path)
    report = aggregate(trades_df)

    report_text = format_report(report, args.signal, research=args.research)
    print(report_text)

    report_path = output_dir / "report.txt"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"報告：{report_path}")

    per_stock_path = output_dir / "per_stock.parquet"
    report.per_stock.to_parquet(per_stock_path, index=False)
    print(f"個股統計：{per_stock_path}")


if __name__ == "__main__":
    main()
