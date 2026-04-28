"""
Aggregation statistics for signal backtest results.

Per-stock:    交易數、勝率、平均報酬、累計報酬、最大回撤
Market-wide:  總交易數、勝率、報酬分布 (mean/median/p25/p75/std)
              、做多 vs 做空對比
              、累計報酬 Top/Bottom 排行

Cumulative return uses (1 + r).cumprod() across the trade sequence
(trade order = entry_date), so it represents the equity curve of a
trader who reinvests the full pct return into the next signal.
Max drawdown is computed on this curve.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class PerStockStats:
    股票代號: str
    股票名稱: str
    方向: str
    交易數: int
    勝率: float
    平均報酬: float
    累計報酬: float
    最大回撤: float


@dataclass
class SideStats:
    """Aggregated stats for one side across all stocks."""
    方向: str
    總交易數: int
    勝率: float
    平均報酬: float
    較差25: float           # 25th percentile (worse-tail boundary)
    中位數報酬: float
    較佳25: float           # 75th percentile (better-tail boundary)


@dataclass
class AggregateReport:
    per_stock: pd.DataFrame              # PerStockStats rows
    side_stats: dict[str, SideStats]     # "做多" / "做空" -> SideStats
    top_long: pd.DataFrame               # by 累計報酬 desc
    bottom_long: pd.DataFrame
    top_short: pd.DataFrame
    bottom_short: pd.DataFrame
    worst_long: pd.Series | None = None  # the single worst long trade (min 報酬率)
    worst_short: pd.Series | None = None


def _compute_drawdown_on_returns(returns: np.ndarray) -> tuple[float, float]:
    """Cumulative return + max drawdown from a sequence of pct returns.

    Returns (cumulative_return, max_drawdown).
    Drawdown is negative or zero.
    """
    if len(returns) == 0:
        return 0.0, 0.0
    equity = np.cumprod(1.0 + returns)
    cum_ret = float(equity[-1] - 1.0)
    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak
    return cum_ret, float(drawdown.min())


def aggregate(trades_df: pd.DataFrame, top_n: int = 20) -> AggregateReport:
    """Compute per-stock and market-wide stats from a flat trade DataFrame."""
    if trades_df.empty:
        empty = pd.DataFrame(columns=[
            "股票代號", "股票名稱", "方向",
            "交易數", "勝率", "平均報酬", "累計報酬", "最大回撤",
        ])
        return AggregateReport(
            per_stock=empty,
            side_stats={},
            top_long=empty,
            bottom_long=empty,
            top_short=empty,
            bottom_short=empty,
        )

    # Order trades within each (stock, side) group by entry_date so the
    # cumulative-return / drawdown calc walks the actual trade sequence.
    trades_df = trades_df.sort_values(["股票代號", "方向", "進場日期"])

    per_stock_rows: list[PerStockStats] = []
    for (sid, side), grp in trades_df.groupby(["股票代號", "方向"], sort=False):
        returns = grp["報酬率"].to_numpy(dtype=np.float64)
        n = len(returns)
        wins = (returns > 0).sum()
        cum, dd = _compute_drawdown_on_returns(returns)
        per_stock_rows.append(PerStockStats(
            股票代號=sid,
            股票名稱=grp["股票名稱"].iloc[0],
            方向=side,
            交易數=n,
            勝率=float(wins / n) if n else 0.0,
            平均報酬=float(returns.mean()) if n else 0.0,
            累計報酬=cum,
            最大回撤=dd,
        ))

    per_stock_df = pd.DataFrame([s.__dict__ for s in per_stock_rows])

    side_stats: dict[str, SideStats] = {}
    for side, grp in trades_df.groupby("方向", sort=False):
        returns = grp["報酬率"].to_numpy(dtype=np.float64)
        n = len(returns)
        wins = (returns > 0).sum()
        side_stats[side] = SideStats(
            方向=side,
            總交易數=n,
            勝率=float(wins / n),
            平均報酬=float(returns.mean()),
            較差25=float(np.percentile(returns, 25)),
            中位數報酬=float(np.median(returns)),
            較佳25=float(np.percentile(returns, 75)),
        )

    def _slice(side: str, ascending: bool) -> pd.DataFrame:
        sub = per_stock_df[per_stock_df["方向"] == side]
        if sub.empty:
            return sub
        return sub.sort_values("累計報酬", ascending=ascending).head(top_n)

    def _worst(side: str) -> pd.Series | None:
        sub = trades_df[trades_df["方向"] == side]
        if sub.empty:
            return None
        return sub.loc[sub["報酬率"].idxmin()]

    return AggregateReport(
        per_stock=per_stock_df,
        side_stats=side_stats,
        top_long=_slice("做多", ascending=False),
        bottom_long=_slice("做多", ascending=True),
        top_short=_slice("做空", ascending=False),
        bottom_short=_slice("做空", ascending=True),
        worst_long=_worst("做多"),
        worst_short=_worst("做空"),
    )
