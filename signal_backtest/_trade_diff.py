"""Per-trade diff between two archived versions (`tdiff` subcommand).

Reads {snapshot_path}/{signal}/trades.parquet from two versions, merges
on (code, entry_date) and prints five sections per signal:

  1. Overview        — removed / added / changed / unchanged counts + pnl sums
  2. Yearly attribution — dCum bucketed by entry year (Rubric 5 mechanised:
                          warn when a single year exceeds 50% of total dCum)
  3. Victim stocks A — common trades whose net degraded by more than 5pp
  4. Victim stocks B — trades new in version B with net below -10pp
  5. Month concentration — entry-month distribution of victims (A union B)

Units: net = raw_pnl - cost_pct (each version uses its own cost), and all
display / thresholds are x100 percent points, matching the SKILL 1a
inline template (-5 / -10 thresholds). Raw sums (x100) reconcile with
`metrics.cum` of the versions DB.

Invoked via: python -m signal_backtest._versions tdiff vA vB --signal X
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd

# UTF-8 on Windows console (idempotent: skipped if _versions already wrapped)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


# Canonical rename: Chinese parquet columns -> English (SKILL 1a template)
CANON = {
    "股票代號": "code",
    "股票名稱": "name",
    "方向": "side",
    "進場日期": "entry_date",
    "進場價": "entry_price",
    "出場日期": "exit_date",
    "出場價": "exit_price",
    "出場原因": "exit_reason",
    "持倉天數": "days",
    "報酬率": "pnl",
    "防守價變化": "defense_events",
}

# Archive layout can hold up to 8 signal subdirs
ALL_SIGNALS = [
    "pick", "touch", "buy", "sell",
    "buy_flee", "sell_flee", "unified_long", "unified_short",
]

CHANGED_EPS_PP = 0.01     # |d raw pnl| threshold (percent points) for "changed"
VICTIM_A_PP = -5.0        # common-trade net degradation threshold
VICTIM_B_PP = -10.0       # new-trade net loss threshold


def _load(snapshot: Path, signal: str, cost_pct: float) -> pd.DataFrame:
    df = pd.read_parquet(snapshot / signal / "trades.parquet")
    df = df.rename(columns=CANON)
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["exit_reason"] = df["exit_reason"].fillna("")
    df["raw"] = df["pnl"] * 100.0                    # raw pnl in pp
    df["net"] = (df["pnl"] - cost_pct) * 100.0       # net pnl in pp
    keep = ["code", "name", "entry_date", "exit_reason", "days", "raw", "net"]
    return df[keep]


def _available_signals(snapshot: Path) -> list[str]:
    if not snapshot.exists():
        return []
    return sorted(
        d.name for d in snapshot.iterdir()
        if d.is_dir() and (d / "trades.parquet").exists()
    )


def _fmt_date(ts) -> str:
    return ts.strftime("%Y-%m-%d")


def _print_overview(va, vb, old, new, removed, added, changed, unchanged_n,
                    cost_a, cost_b) -> None:
    cum_old = old["raw"].sum()
    cum_new = new["raw"].sum()
    print(f"【概覽】raw pnl 合計：{va} {cum_old:+.2f}pp → {vb} {cum_new:+.2f}pp"
          f"（Δcum {cum_new - cum_old:+.2f}pp）")
    if cost_a != cost_b:
        print(f"  ⚠ 兩版成本假設不同：{va}={cost_a*100:.2f}% vs {vb}={cost_b*100:.2f}%"
              f"，net 各用各版成本")
    print(f"  消失（僅 {va}）：{len(removed):>4} 筆  raw 合計 {removed['raw_old'].sum():+.2f}pp")
    print(f"  新增（僅 {vb}）：{len(added):>4} 筆  raw 合計 {added['raw_new'].sum():+.2f}pp")
    d_common = (changed["raw_new"] - changed["raw_old"]).sum()
    print(f"  共同且變化    ：{len(changed):>4} 筆  Δraw 合計 {d_common:+.2f}pp")
    print(f"  共同未變      ：{unchanged_n:>4} 筆")


def _print_yearly(va, vb, old, new) -> None:
    g_old = old.groupby(old["entry_date"].dt.year)["raw"].agg(["count", "sum"])
    g_new = new.groupby(new["entry_date"].dt.year)["raw"].agg(["count", "sum"])
    years = sorted(set(g_old.index) | set(g_new.index))
    total = new["raw"].sum() - old["raw"].sum()
    print("\n【年度歸因】（Δcum = 該年 raw pnl 合計之差，pp）")
    print(f"  {'年份':<6}{'Δn':>6}{'Δcum':>10}{'佔總Δ%':>9}")
    warn_years = []
    for y in years:
        n_o = int(g_old["count"].get(y, 0))
        n_n = int(g_new["count"].get(y, 0))
        c_o = float(g_old["sum"].get(y, 0.0))
        c_n = float(g_new["sum"].get(y, 0.0))
        d = c_n - c_o
        if total != 0:
            share = d / total * 100
            share_s = f"{share:>8.1f}%"
            if share > 50.0:
                warn_years.append((y, share))
        else:
            share_s = "      —"
        print(f"  {y:<6}{n_n - n_o:>+6d}{d:>+10.2f}{share_s}")
    for y, share in warn_years:
        print(f"  ⚠ 單一年份 {y} 佔總 Δcum {share:.0f}% > 50%——增益/損失集中"
              f"單一時段，檢查是否 overfit（Rubric 5 時間均勻）")


def _print_victims_a(victims_a: pd.DataFrame) -> None:
    print(f"\n【受害股 A】共同交易 net 退步 < {VICTIM_A_PP:.0f}pp"
          f"（共 {len(victims_a)} 筆，列 top 10）")
    if victims_a.empty:
        print("  （無）")
        return
    top = victims_a.nsmallest(10, "d_net")
    print(f"  {'code':<7}{'name':<8}{'entry_date':<12}{'net_old':>9}{'net_new':>9}"
          f"{'diff':>9}  exit_reason_new")
    for _, r in top.iterrows():
        print(f"  {r['code']:<7}{r['name_old']:<8}{_fmt_date(r['entry_date']):<12}"
              f"{r['net_old']:>+9.2f}{r['net_new']:>+9.2f}{r['d_net']:>+9.2f}"
              f"  {r['exit_reason_new']}")


def _print_victims_b(victims_b: pd.DataFrame) -> None:
    print(f"\n【受害股 B】新增交易 net < {VICTIM_B_PP:.0f}pp"
          f"（共 {len(victims_b)} 筆，列 top 10）")
    if victims_b.empty:
        print("  （無）")
        return
    top = victims_b.nsmallest(10, "net_new")
    print(f"  {'code':<7}{'name':<8}{'entry_date':<12}{'days':>5}{'net':>9}"
          f"  exit_reason")
    for _, r in top.iterrows():
        print(f"  {r['code']:<7}{r['name_new']:<8}{_fmt_date(r['entry_date']):<12}"
              f"{int(r['days_new']):>5}{r['net_new']:>+9.2f}  {r['exit_reason_new']}")


def _print_month_concentration(victims_a: pd.DataFrame,
                               victims_b: pd.DataFrame) -> None:
    dates = pd.concat([victims_a["entry_date"], victims_b["entry_date"]])
    print(f"\n【集中時段觀察】受害交易（A∪B）entry_date 月份分布（共 {len(dates)} 筆）")
    if dates.empty:
        print("  （無受害交易）")
        return
    months = dates.dt.strftime("%Y-%m").value_counts()
    shown = months.head(8)
    for m, cnt in shown.items():
        print(f"  {m}: {cnt} 筆")
    rest = months.iloc[8:]
    if len(rest) > 0:
        print(f"  …其餘 {int(rest.sum())} 筆分散於 {len(rest)} 個月")


def diff_signal(snap_a: Path, snap_b: Path, signal: str,
                va: str, vb: str, cost_a: float, cost_b: float) -> None:
    """Diff one signal's trades between two version snapshots and print report."""
    old = _load(snap_a, signal, cost_a)
    new = _load(snap_b, signal, cost_b)

    m = old.merge(new, on=["code", "entry_date"], how="outer",
                  suffixes=("_old", "_new"), indicator=True)
    removed = m[m["_merge"] == "left_only"]
    added = m[m["_merge"] == "right_only"]
    common = m[m["_merge"] == "both"].copy()
    common["d_raw"] = common["raw_new"] - common["raw_old"]
    common["d_net"] = common["net_new"] - common["net_old"]
    changed_mask = (common["d_raw"].abs() > CHANGED_EPS_PP) | \
                   (common["exit_reason_old"] != common["exit_reason_new"])
    changed = common[changed_mask]
    unchanged_n = len(common) - len(changed)

    victims_a = common[common["d_net"] < VICTIM_A_PP]
    victims_b = added[added["net_new"] < VICTIM_B_PP]

    print(f"\n=== tdiff {va} → {vb}  [{signal}] ===")
    _print_overview(va, vb, old, new, removed, added, changed, unchanged_n,
                    cost_a, cost_b)
    _print_yearly(va, vb, old, new)
    _print_victims_a(victims_a)
    _print_victims_b(victims_b)
    _print_month_concentration(victims_a, victims_b)


def run_tdiff(snap_a: Path, snap_b: Path, va: str, vb: str,
              cost_a: float, cost_b: float,
              signal: str | None = None, all_signals: bool = False) -> None:
    if all_signals:
        avail_a = set(_available_signals(snap_a))
        avail_b = set(_available_signals(snap_b))
        for sig in ALL_SIGNALS:
            if sig not in avail_a or sig not in avail_b:
                missing = [v for v, av in ((va, avail_a), (vb, avail_b))
                           if sig not in av]
                print(f"\n=== tdiff {va} → {vb}  [{sig}] ===")
                print(f"  跳過：{'、'.join(missing)} 缺 {sig}/trades.parquet")
                continue
            diff_signal(snap_a, snap_b, sig, va, vb, cost_a, cost_b)
        return

    for v, snap in ((va, snap_a), (vb, snap_b)):
        if not (snap / signal / "trades.parquet").exists():
            avail = _available_signals(snap)
            print(f"{v} 缺 {signal}/trades.parquet（快照 {snap}）；"
                  f"該版實際存在的訊號：{'、'.join(avail) or '（無）'}",
                  file=sys.stderr)
            sys.exit(1)
    diff_signal(snap_a, snap_b, signal, va, vb, cost_a, cost_b)
