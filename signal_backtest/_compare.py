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

The fill section reads tw.daily_prices (read-only): entries that closed on
the published limit-up (long) or limit-down (short) price, and the
one-price ones among them, with the PF left once they are dropped. Fills are at the signal bar's close
(user ruling 2026-10-19: orders go in from the intraday snapshot), but a
locked limit close may not fill, and a one-price day never does.

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

from signal_backtest.trade import REASON_EXIT_END

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
UNIFIED = [
    ("unified_long",  "合多", "多"),
    ("unified_short", "合空", "空"),
]

# A reference price equal to the close on a day that moved this much is
# almost surely the next day's, stored a day early (limits with it).
SUSPECT_MOVE = 0.05


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


def compute_pool(base_dir: Path) -> dict | None:
    """Pooled stats of the six signals: their trades concatenated, judged
    with compute_stats. None when any signal's trades.parquet is missing --
    five signals' pool is not the pool."""
    frames = []
    for key, _, _ in SIGNALS:
        path = base_dir / key / "trades.parquet"
        if not path.exists():
            return None
        frames.append(pd.read_parquet(path))
    df = pd.concat(frames, ignore_index=True)
    s = compute_stats(df)
    if s is None:
        return None
    return dict(
        n=s["n"],
        win_pct=s["win_pct"],
        pf_net=s["pf_net"],
        payoff=s["win_avg"] / abs(s["loss_avg"]) if s["loss_avg"] else float("inf"),
        data_cutoff=str(pd.to_datetime(df["出場日期"]).max().date()),
    )


def _bar_keys(df: pd.DataFrame, col: str) -> pd.MultiIndex:
    return pd.MultiIndex.from_arrays(
        [df["股票代號"].astype(str), pd.to_datetime(df[col]).dt.date],
        names=["stock_id", "trade_date"])


def _fetch_bars(keys: pd.MultiIndex) -> pd.DataFrame:
    """The bars of keys from tw.daily_prices, indexed (stock_id,
    trade_date): high, low, close, the published limit_up / limit_down and
    ref (NaN where missing), and prev_close -- the last bar with a close
    before it, as backtest.data loads them (NaN on a stock's first bar).
    Read-only."""
    from db.connection import get_cursor

    sids = sorted(set(keys.get_level_values(0)))
    with get_cursor(commit=False) as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            "SELECT p.* FROM (SELECT stock_id, trade_date, high_price AS high, "
            "low_price AS low, close_price AS close, limit_up, limit_down, ref_price AS ref, "
            "lag(close_price) OVER (PARTITION BY stock_id ORDER BY trade_date) AS prev_close "
            "FROM tw.daily_prices WHERE stock_id = ANY(%s) AND close_price IS NOT NULL) p "
            "WHERE (p.stock_id, p.trade_date) IN "
            "(SELECT * FROM unnest(%s::text[], %s::date[]))",
            (sids, [str(s) for s in keys.get_level_values(0)], list(keys.get_level_values(1))))
        rows = cur.fetchall()
    cols = ["high", "low", "close", "limit_up", "limit_down", "ref", "prev_close"]
    bars = pd.DataFrame(rows, columns=["stock_id", "trade_date", *cols])
    bars[cols] = bars[cols].astype(float)
    return bars.set_index(["stock_id", "trade_date"])


def limit_entry_flags(df: pd.DataFrame, side: str, bars: pd.DataFrame) -> pd.DataFrame:
    """Per trade: matched (an entry bar with its published limit on record),
    at_limit (closed on the limit the entry chases: limit_up for 多,
    limit_down for 空), one_price (at_limit with high == low) and suspect
    (ref equals the close on a move of SUSPECT_MOVE or more: the row's ref
    and limits are likely a day off, so at_limit may be wrong)."""
    b = bars.reindex(_bar_keys(df, "進場日期"))
    limit = b["limit_up"] if side == "多" else b["limit_down"]
    close = b["close"].to_numpy()
    matched = b["close"].notna().to_numpy() & limit.notna().to_numpy()
    at_limit = matched & ((close >= limit.to_numpy()) if side == "多"
                          else (close <= limit.to_numpy()))
    one_price = at_limit & (b["high"].to_numpy() == b["low"].to_numpy())
    move = np.abs(close / b["prev_close"].to_numpy() - 1)
    suspect = matched & (b["ref"].to_numpy() == close) & (move >= SUSPECT_MOVE)
    return pd.DataFrame({"matched": matched, "at_limit": at_limit,
                         "one_price": one_price, "suspect": suspect}, index=df.index)


def limit_exit_flags(df: pd.DataFrame, side: str, bars: pd.DataFrame) -> pd.DataFrame:
    """Per trade: exit_matched (the exit bar with its published limit on
    record), exit_at_limit (the exit bar closed on the limit against the
    position: limit_down for 多, limit_up for 空 -- a locked close may not
    fill) and exit_one_price (with high == low: it does not). Trades still
    open at the end of data were not exited; they are matched and never
    flagged."""
    b = bars.reindex(_bar_keys(df, "出場日期"))
    limit = b["limit_down"] if side == "多" else b["limit_up"]
    close = b["close"].to_numpy()
    real = (df["出場原因"] != REASON_EXIT_END).to_numpy()
    found = b["close"].notna().to_numpy() & limit.notna().to_numpy()
    at_limit = real & found & ((close <= limit.to_numpy()) if side == "多"
                               else (close >= limit.to_numpy()))
    one_price = at_limit & (b["high"].to_numpy() == b["low"].to_numpy())
    return pd.DataFrame({"exit_matched": ~real | found, "exit_at_limit": at_limit,
                         "exit_one_price": one_price}, index=df.index)


def _sensitivity_row(df: pd.DataFrame, flags: pd.DataFrame) -> dict:
    def pf(mask):
        s = compute_stats(df[mask.to_numpy()])
        return s["pf_net"] if s else float("nan")

    keep = pd.Series(True, index=df.index)
    return dict(
        n=len(df),
        at_limit=int(flags["at_limit"].sum()),
        one_price=int(flags["one_price"].sum()),
        suspect=int(flags["suspect"].sum()),
        unmatched=int((~(flags["matched"] & flags["exit_matched"])).sum()),
        exit_at_limit=int(flags["exit_at_limit"].sum()),
        exit_one_price=int(flags["exit_one_price"].sum()),
        pf_all=pf(keep),
        pf_no_one=pf(~flags["one_price"]),
        pf_no_limit=pf(~flags["at_limit"]),
    )


def limit_sensitivity(base_dir: Path) -> list[tuple[str, str, dict]]:
    """Fill-sensitivity rows for the six signals, their pool (only when all
    six are there, as compute_pool) and the two unified sides present."""
    frames = {}
    for key, label, side in SIGNALS + UNIFIED:
        path = base_dir / key / "trades.parquet"
        if path.exists():
            frames[key] = (label, side, pd.read_parquet(path))
    keys = sorted({k for _, _, df in frames.values()
                   for col in ("進場日期", "出場日期") for k in _bar_keys(df, col)})
    bars = _fetch_bars(pd.MultiIndex.from_tuples(keys)) if keys else None

    six = {k for k, _, _ in SIGNALS}
    rows, pool = [], []
    for key, (label, side, df) in frames.items():
        if df.empty:
            rows.append((label, side, None))      # printed as no trades
            continue
        flags = limit_entry_flags(df, side, bars).join(limit_exit_flags(df, side, bars))
        rows.append((label, side, _sensitivity_row(df, flags)))
        if key in six:
            pool.append((df, flags))
    n_six = len(six & frames.keys())
    if n_six == len(SIGNALS) and pool:
        df = pd.concat([d for d, _ in pool], ignore_index=True)
        flags = pd.concat([f for _, f in pool], ignore_index=True)
        rows.insert(n_six, ("合池", "", _sensitivity_row(df, flags)))
    return rows


def compare(base_dir: Path) -> bool:
    """Print the report; False when the fill section could not be computed."""
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

    print("\n【成交可行性】收在公告漲／跌停價的進場（多看漲停、空看跌停；一字＝最高等於最低）")
    try:
        sens = limit_sensitivity(base_dir)
    except Exception as e:
        print(f"  查價失敗，本段沒有算：{type(e).__name__}: {e}")
        return False
    print(f"{'訊號':<8}{'方向':<5}{'交易':>7}{'漲跌停':>7}{'%':>6}{'一字':>6}"
          f"{'PF淨':>7}{'剔一字':>7}{'剔漲跌停':>9}{'對不到':>7}{'參考價疑錯':>9}"
          f"{'出場鎖價':>8}{'出場一字':>8}")
    print("-" * 101)
    for label, side, r in sens:
        if r is None:
            print(f"{label:<8}{side:<5} no trades")
            continue
        print(f"{label:<8}{side:<5}{r['n']:>7}{r['at_limit']:>7}"
              f"{r['at_limit'] / r['n'] * 100:>6.1f}{r['one_price']:>6}"
              f"{r['pf_all']:>7.3f}{r['pf_no_one']:>7.3f}{r['pf_no_limit']:>9.3f}"
              f"{r['unmatched']:>7}{r['suspect']:>9}"
              f"{r['exit_at_limit']:>8}{r['exit_one_price']:>8}")
    print("剔除後是敏感度的一端，不是真實值；一字確定買不到。PF 印 inf＝該組沒有虧損單，nan＝該組沒有交易。")
    print("對不到＝進場或出場 K 棒、或其公告漲跌停價不在 DB。")
    print(f"參考價疑錯＝參考價等於收盤且當天漲跌 ≥{SUSPECT_MOVE:.0%}：該列參考價與漲跌停價疑似存成隔天的值，"
          "只會漏判漲跌停（不會多判），所以漲跌停 % 可能偏低、剔漲跌停 PF 可能偏高。")
    print("出場鎖價＝出場日收在不利方向的公告漲跌停價（多看跌停、空看漲停），收盤可能賣不出；"
          "出場一字＝其中最高等於最低，確定出不了場。只列筆數，PF 沒有反映（回測結束的強平不算出場）。")
    print("本段沒量到：做空的融券停券、平盤下不得放空等限制。")
    return True


def main() -> None:
    if len(sys.argv) > 1:
        base_dir = Path(sys.argv[1])
    else:
        base_dir = Path("tmp/sb_compare")

    if not base_dir.exists():
        print(f"目錄不存在：{base_dir}", file=sys.stderr)
        sys.exit(1)

    if not compare(base_dir):
        sys.exit(1)


if __name__ == "__main__":
    main()
