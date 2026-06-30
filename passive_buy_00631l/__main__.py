"""CLI: python -m passive_buy_00631l run

Runs M1→M5: load data, build signals, backtest 4 cohorts, print/save report + charts.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import yaml

from .data_layer import load_market_frame
from .signal_layer import build_signals
from .backtest_layer import run_backtests
from .metrics import leverage_decay_gap
from . import report_layer as RPT


def load_config():
    path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def cmd_run(cfg):
    print("[1/5] 載入資料 ...")
    frame = load_market_frame(cfg)
    print(f"      {frame.index[0].date()} ~ {frame.index[-1].date()}  {len(frame)} 交易日")

    print("[2/5] 計算便宜訊號 ...")
    signals = build_signals(frame, cfg)

    print("[3/5] 回測 4 組對照 ...")
    results = run_backtests(frame, signals, cfg)

    print("[4/5] 計算指標 ...")
    mets = RPT.compute_metrics(results)
    fwd = RPT.forward_return_check(results, frame, cfg["report"]["forward_days"])
    _, _, decay_gap = leverage_decay_gap(
        frame["c_close"].to_numpy(dtype=np.float64), frame["target"].to_numpy(dtype=np.float64))
    report = RPT.build_text_report(results, mets, fwd, decay_gap, cfg, frame)
    m4 = RPT.build_m4_report(frame, signals, cfg)
    bear = RPT.build_fullrun_bear_report(results, frame)
    report = report + "\n" + m4 + "\n" + bear
    print("\n" + report)

    print("\n[5/5] 輸出報表與圖 ...")
    outdir = os.path.join(os.path.dirname(__file__), cfg["report"]["outdir"])
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "report.txt"), "w", encoding="utf-8") as f:
        f.write(report)
    RPT.make_charts(results, signals, frame, cfg, outdir)
    print(f"      已輸出至 {outdir}")


def cmd_experiments(cfg):
    from .experiments import (run_experiments, run_rebalance_experiments,
                              run_signal_rebalance_experiments, run_hybrid_experiments,
                              run_voltarget_experiments, run_cost_experiments,
                              run_dirvol_experiments)
    print("載入資料與訊號 ...")
    frame = load_market_frame(cfg)
    signals = build_signals(frame, cfg)
    report = run_experiments(frame, signals, cfg)
    report += "\n" + run_rebalance_experiments(frame, cfg)
    report += "\n" + run_signal_rebalance_experiments(frame, signals, cfg)
    report += "\n" + run_hybrid_experiments(frame, signals, cfg)
    report += "\n" + run_voltarget_experiments(frame, signals, cfg)
    report += "\n" + run_dirvol_experiments(frame, signals, cfg)
    report += "\n" + run_cost_experiments(frame, signals, cfg)
    print(report)
    outdir = os.path.join(os.path.dirname(__file__), cfg["report"]["outdir"])
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "experiments.txt"), "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n已輸出至 {os.path.join(outdir, 'experiments.txt')}")


def main():
    cfg = load_config()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        cmd_run(cfg)
    elif cmd == "experiments":
        cmd_experiments(cfg)
    else:
        print(f"未知指令: {cmd}（可用: run, experiments）")
        sys.exit(1)


if __name__ == "__main__":
    main()
