"""Weekly 籌碼 (shareholding) change metrics over a fixed lookback window.

The strategy ranks stocks cross-sectionally on three independent chip dimensions
(see strategy.py); this module computes those dimensions per (stock_id, week).
Tier boundaries: 散戶 <10張 = t1..t3, 大戶 >800張 = t14..t15, 千張大戶 = t15.
"""
import pandas as pd

from chip_model.db_access import load_distribution

WINDOW_WEEKS = 4  # lookback for every change metric


def compute_metrics(dist: pd.DataFrame | None = None) -> pd.DataFrame:
    """Per (stock_id, data_date):

    ratio     — 千張大戶比例 (t15_pct, %), context only.
    d_big     — 大戶 (>800張 = t14+t15) 比例的 4 週累積增幅 (pct-point).
    d_retail  — 散戶 (<10張 = t1+t2+t3) 比例的 4 週「減少」量 (正 = 散戶退場).
    d_holders — 千張大戶 (t15) 持有人數的 4 週增加數.

    The three deltas are NaN until each stock has WINDOW_WEEKS+1 weeks of history.
    """
    if dist is None:
        dist = load_distribution()
    df = dist.copy()
    for c in ["t1_pct", "t2_pct", "t3_pct", "t14_pct", "t15_pct"]:
        df[c] = df[c].astype(float)
    df["ratio"] = df["t15_pct"]
    df["big"] = df["t14_pct"] + df["t15_pct"]
    df["retail"] = df["t1_pct"] + df["t2_pct"] + df["t3_pct"]
    df["holders"] = df["t15_holders"].astype(float)

    df = df.sort_values(["stock_id", "data_date"]).reset_index(drop=True)
    g = df.groupby("stock_id")
    k = WINDOW_WEEKS
    # Source pct is NUMERIC(6,2); round pct diffs back to 2 dp to drop float noise.
    df["d_big"] = g["big"].diff(k).round(2)
    df["d_retail"] = (-g["retail"].diff(k)).round(2)
    df["d_holders"] = g["holders"].diff(k)
    return df[["stock_id", "data_date", "ratio",
               "d_big", "d_retail", "d_holders"]]
