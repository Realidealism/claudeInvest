"""籌碼集中度選股 (pure shareholding-change rule; no price / sector input).

Each week, rank the common-stock universe on three independent chip dimensions
over a 4-week window, then pick the top N by their consensus (average rank):

  1. d_big     大戶持股增加   (>800張 = t14+t15 比例增幅)
  2. d_retail  散戶持股減少   (<10張  = t1+t2+t3 比例減少)
  3. d_holders 千張大戶人數增 (t15 持有人數增加)

Rationale (backtest sweep, pure 股權變化): each dimension alone already beats
TAIEX on excess return, and the 3-way consensus is far stronger and more robust
than any single metric (12週 勝率 ~66%, 超額 ~+11%). It captures chips moving
from weak (散戶) to strong (大戶) hands. The earlier "比例 level" gate was a
reverse indicator and was dropped.
"""
from dataclasses import dataclass

import pandas as pd

DIMENSIONS = ["d_big", "d_retail", "d_holders"]


@dataclass(frozen=True)
class Rule:
    top_n: int = 20   # 每週取籌碼集中度前 N 名


def generate_signals(metrics: pd.DataFrame, rule: Rule, universe: set[str]) -> pd.DataFrame:
    """All (stock_id, data_date) rows picked by the consensus rank.

    Ranking is cross-sectional within the common-stock universe per week.
    Same input -> same output (sorted by date, then score desc).
    """
    m = metrics[metrics["stock_id"].isin(universe)].copy()
    m = m.dropna(subset=DIMENSIONS)
    # Consensus: sum of per-week descending ranks across the 3 dimensions
    # (lower rank-sum = stronger). Negate so higher score = better.
    rank_sum = sum(
        m.groupby("data_date")[d].rank(ascending=False, method="average")
        for d in DIMENSIONS
    )
    m["score"] = -rank_sum
    m["pick_rank"] = m.groupby("data_date")["score"].rank(
        ascending=False, method="first"
    )
    sig = m[m["pick_rank"] <= rule.top_n]
    cols = ["stock_id", "data_date", "ratio", "d_big", "d_retail", "d_holders", "score"]
    return (
        sig[cols]
        .sort_values(["data_date", "score"], ascending=[True, False])
        .reset_index(drop=True)
    )


def pick_for_date(signals: pd.DataFrame, data_date) -> pd.DataFrame:
    """Picks for a single snapshot week."""
    return signals[signals["data_date"] == data_date].reset_index(drop=True)
