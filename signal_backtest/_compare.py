"""Cross-signal aggregation utility.

Reads trades.parquet from multiple signal-backtest output dirs and prints
a side-by-side comparison table aligned to the three improvement goals
(see feedback_signal_improvement_goals.md):

  1. 提高勝率              → 勝率%
  2. 降低 max 虧損 / 提升 max 獲利
                           → 最大單筆虧損% / 最大單筆獲利% / 最大連續回撤%
  3. 提升交易質量、減少次數  → 交易數 / PF / 淨均報%

淨均報% assumes a 0.4% round-trip cost (TW: commission + tax). Edit
COST_PCT below if your real cost differs.

Usage:
    python -m signal_backtest._compare                          # default dirs
    python -m signal_backtest._compare tmp/sb_versions/v0_baseline
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Force UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


COST_PCT = 0.004  # 0.4% round-trip transaction cost

SIGNALS = [
    ("pick",      "抄底",   "多"),
    ("touch",     "摸頭",   "空"),
    ("buy",       "波段多", "多"),
    ("sell",      "波段空", "空"),
    ("buy_flee",  "多翻空", "空"),
    ("sell_flee", "空翻多", "多"),
]


def _max_drawdown(pnl_chrono: np.ndarray) -> float:
    """Max drawdown of equity curve assuming equal-capital per trade.

    Trades are summed (not compounded) so the drawdown is in 'units of
    single-trade-pnl-pct'. Cross-signal comparable.
    """
    if len(pnl_chrono) == 0:
        return 0.0
    equity = np.cumsum(pnl_chrono)
    running_max = np.maximum.accumulate(equity)
    dd = running_max - equity
    return float(dd.max())


def compute_stats(df: pd.DataFrame) -> dict | None:
    if df.empty:
        return None

    pnl_raw = df["報酬率"].values.astype(np.float64)
    pnl_net = pnl_raw - COST_PCT
    n = len(df)
    win = (pnl_raw > 0).sum()
    win_pct = win / n * 100

    # Chronological order for drawdown
    df_sorted = df.sort_values("出場日期")
    pnl_chrono = df_sorted["報酬率"].values.astype(np.float64)
    pnl_chrono_net = pnl_chrono - COST_PCT
    mdd_raw = _max_drawdown(pnl_chrono) * 100
    mdd_net = _max_drawdown(pnl_chrono_net) * 100

    win_avg = pnl_raw[pnl_raw > 0].mean() * 100 if win > 0 else 0.0
    loss_avg = pnl_raw[pnl_raw < 0].mean() * 100 if (n - win) > 0 else 0.0
    pf_raw = (
        abs(pnl_raw[pnl_raw > 0].sum() / pnl_raw[pnl_raw < 0].sum())
        if (pnl_raw < 0).any() else float("inf")
    )
    pf_net = (
        abs(pnl_net[pnl_net > 0].sum() / pnl_net[pnl_net < 0].sum())
        if (pnl_net < 0).any() else float("inf")
    )

    return dict(
        n=n,
        win_pct=win_pct,
        avg_raw=pnl_raw.mean() * 100,
        avg_net=pnl_net.mean() * 100,
        median=np.median(pnl_raw) * 100,
        cum=pnl_raw.sum() * 100,
        max_win=pnl_raw.max() * 100,
        max_loss=pnl_raw.min() * 100,
        mdd=mdd_raw,
        mdd_net=mdd_net,
        avg_hold=df["持倉天數"].mean(),
        win_avg=win_avg,
        loss_avg=loss_avg,
        pf_raw=pf_raw,
        pf_net=pf_net,
    )


def compare(base_dir: Path) -> None:
    print(f"成本假設：來回 {COST_PCT*100:.1f}% (TW 手續費+證交稅)")
    print(f"資料來源：{base_dir}\n")

    rows: list[tuple] = []
    for key, label, side in SIGNALS:
        path = base_dir / key / "trades.parquet"
        if not path.exists():
            print(f"  缺檔: {path}")
            continue
        df = pd.read_parquet(path)
        s = compute_stats(df)
        if s is None:
            print(f"  {label:<8} {side:<3} no trades")
            continue
        rows.append((label, side, s))

    # Goal 1 + 3: 勝率 / 交易質量
    print("【目標 1+3】勝率 + 交易質量（少而精）")
    print(f"{'訊號':<8}{'方向':<5}{'交易':>7}{'勝率%':>7}{'毛均%':>7}"
          f"{'淨均%':>7}{'PF毛':>6}{'PF淨':>6}{'持倉':>5}")
    print("-" * 65)
    for label, side, s in rows:
        net_marker = "(!)" if s["pf_net"] < 1.0 else "   "
        print(f"{label:<8}{side:<5}{s['n']:>7}{s['win_pct']:>7.1f}"
              f"{s['avg_raw']:>+7.2f}{s['avg_net']:>+7.2f}"
              f"{s['pf_raw']:>6.2f}{s['pf_net']:>6.2f} {net_marker}{s['avg_hold']:>5.0f}")

    # Goal 2: 左尾右尾
    print("\n【目標 2】左尾右尾 + 連續回撤")
    print(f"{'訊號':<8}{'方向':<5}{'最大獲利%':>10}{'最大虧損%':>10}"
          f"{'贏均%':>7}{'輸均%':>7}{'最大連續回撤%':>14}{'淨':>14}")
    print("-" * 80)
    for label, side, s in rows:
        print(f"{label:<8}{side:<5}{s['max_win']:>+10.1f}{s['max_loss']:>+10.1f}"
              f"{s['win_avg']:>+7.2f}{s['loss_avg']:>+7.2f}"
              f"{s['mdd']:>13.1f} {s['mdd_net']:>13.1f}")

    print("\n標記：(!) = 扣成本後 PF<1（無 edge）")


def main() -> None:
    if len(sys.argv) > 1:
        base_dir = Path(sys.argv[1])
    else:
        base_dir = Path("tmp/sb_compare")

    if not base_dir.exists():
        print(f"目錄不存在：{base_dir}", file=sys.stderr)
        sys.exit(1)

    compare(base_dir)


if __name__ == "__main__":
    main()
