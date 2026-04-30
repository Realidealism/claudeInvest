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
from signal_backtest.batch import run_batch
from signal_backtest.aggregate import aggregate
from signal_backtest.report import format_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Signal backtester (multi-stock)")
    p.add_argument(
        "--signal",
        required=True,
        help=f"Signal factory name. Options: {', '.join(SIGNAL_FACTORIES)}",
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
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.signal not in SIGNAL_FACTORIES:
        print(f"未知訊號：{args.signal}", file=sys.stderr)
        print(f"可用訊號：{', '.join(SIGNAL_FACTORIES)}", file=sys.stderr)
        sys.exit(1)

    factory = SIGNAL_FACTORIES[args.signal]
    output_dir = args.output or Path("tmp/signal_backtest") / args.signal

    stock_ids = (
        [s.strip() for s in args.stocks.split(",") if s.strip()]
        if args.stocks else None
    )

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
