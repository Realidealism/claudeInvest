"""Optimization experiments (buy-only, within SPEC):
  1) 動態資金拆分 — base_ratio scales with cheapness (expensive→hoard, cheap→buy)
  2) 集中彈藥打崩盤 — uncapped pool, fire only on deep dips
  3) 換 regime 評估 — fresh re-run inside choppy/bear windows (pool starts empty)

Reuses the deploy logic; a single-cohort accumulation engine keeps the clean
4-cohort backtest untouched. Compared against pure DCA-A (the benchmark).
"""
from __future__ import annotations

import numpy as np

from .backtest_layer import _month_starts
from .strategy_layer import deploy_fraction, should_deploy
from . import metrics as M

INF_CAP = 1e15


def _run_accum(dates, target, cheap, choppy, M_in, base_ratio_fn,
               pool_cap, threshold, tiers, stale_months, stale_frac):
    """Single buy-only accumulating cohort. base_ratio_fn(i)->[0,1] at month starts.
    base_ratio_fn == None means pure DCA (full M into target, no pool)."""
    n = len(dates)
    ms = _month_starts(dates)
    units = cash = basis = 0.0
    value = np.zeros(n)
    cf = []
    months_since = 0
    for i in range(n):
        px = target[i]
        if ms[i]:
            months_since += 1
            if base_ratio_fn is None:
                units += M_in / px
                basis += M_in
            else:
                br = base_ratio_fn(i)
                B = M_in * br
                R = M_in - B
                units += B / px
                basis += B
                pool = cash + R
                if pool > pool_cap:
                    overflow = pool - pool_cap
                    units += overflow / px
                    basis += overflow
                    pool = pool_cap
                cash = pool
                if months_since >= stale_months and cash > 0:
                    amt = cash * stale_frac
                    units += amt / px
                    basis += amt
                    cash -= amt
                    months_since = 0
            cf.append((dates[i], -M_in))
        if base_ratio_fn is not None and cash > 0:
            sc = cheap[i]
            if not np.isnan(sc) and should_deploy(sc, bool(choppy[i]), threshold):
                amt = cash * deploy_fraction(sc, tiers)
                if amt > 0:
                    units += amt / px
                    basis += amt
                    cash -= amt
                    months_since = 0
        value[i] = units * px + cash
    cf.append((dates[-1], value[-1]))
    return {
        "value": value, "units": units, "basis": basis, "cash": cash,
        "final": float(value[-1]),
        "invested": float(-sum(a for _, a in cf[:-1])),
        "xirr": M.xirr([d for d, _ in cf], [a for _, a in cf]),
        "max_dd": M.max_drawdown(value),
        "avg_cost": basis / units if units > 0 else float("nan"),
    }


def _pct(x):
    return "n/a" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x*100:.2f}%"


def _run_rebalance(dates, target, safe_price, M_in, w_target, band):
    """Fixed-ratio rebalance between 00631L and a safe leg. BREAKS buy-only:
    trims 00631L when overweight (sells), tops up when underweight. Rebalances on
    band breach or at each monthly contribution.
    safe_price: per-day price of the safe leg. Pass ones(n) for cash (0% return),
    or 0050 split-adjusted close for an appreciating safe leg."""
    n = len(dates)
    ms = _month_starts(dates)
    units_a = units_b = 0.0  # a = 00631L, b = safe leg (cash ⇒ price≡1 ⇒ units_b=cash)
    value = np.zeros(n)
    cf = []
    sells = 0
    sell_amt = 0.0
    for i in range(n):
        pa = target[i]
        pb = safe_price[i]
        if ms[i]:
            units_b += M_in / pb
            cf.append((dates[i], -M_in))
        va = units_a * pa
        vb = units_b * pb
        total = va + vb
        if total > 0:
            w = va / total
            if ms[i] or abs(w - w_target) > band:
                delta = total * w_target - va  # >0 buy 00631L, <0 sell
                units_a += delta / pa
                units_b -= delta / pb
                if delta < 0:
                    sells += 1
                    sell_amt += -delta
        value[i] = units_a * pa + units_b * pb
    cf.append((dates[-1], value[-1]))
    xirr = M.xirr([d for d, _ in cf], [a for _, a in cf])
    mdd = M.max_drawdown(value)
    return {
        "final": float(value[-1]),
        "invested": float(-sum(a for _, a in cf[:-1])),
        "xirr": xirr,
        "max_dd": mdd,
        "calmar": (xirr / abs(mdd)) if (xirr is not None and mdd < 0) else float("nan"),
        "sells": sells,
        "sell_amt": sell_amt,
    }


def run_rebalance_experiments(frame, cfg) -> str:
    """固定比例再平衡（含賣出，跨出只買不賣框架）。安全腿比較 現金 vs 0050。
    以 Calmar/回撤評估，非總報酬。"""
    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    cash_leg = np.ones(n)
    leg0050 = frame["c_close"].to_numpy(dtype=np.float64)
    Min = cfg["capital"]["monthly_input"]
    band = 0.05

    L = ["", "=" * 88, "再平衡實驗（固定比例 00631L:安全腿，含賣出 — 跨出 SPEC 只買不賣）", "=" * 88]
    a = _run_accum(dates, target, np.zeros(n), np.zeros(n, bool),
                   Min, None, INF_CAP, 1.0, cfg["signal"]["add_tiers"], 999, 0.0)
    a_calmar = (a["xirr"] / abs(a["max_dd"])) if a["max_dd"] < 0 else float("nan")
    L.append("  評估重點：最大回撤↓ 與 Calmar↑（多頭總報酬必輸純抱，看風險調整）")
    L.append(f"{'配置':<32}{'期末市值':>12}{'XIRR':>9}{'vs A':>9}{'最大回撤':>10}{'Calmar':>9}{'賣出':>7}")
    L.append("-" * 90)
    L.append(f"{'A 純DCA 00631L(不再平衡)':<32}{a['final']:>12.0f}{_pct(a['xirr']):>9}"
             f"{'0.00%':>9}{_pct(a['max_dd']):>10}{a_calmar:>9.2f}{0:>7}")
    for leg_name, leg in [("現金", cash_leg), ("0050", leg0050)]:
        for w in [0.40, 0.50, 0.60, 0.75]:
            r = _run_rebalance(dates, target, leg, Min, w, band)
            vs_a = r["final"] / a["final"] - 1.0
            mark = " ★" if (not np.isnan(r["calmar"]) and r["calmar"] > a_calmar) else ""
            L.append(f"{f'再平衡 {int(w*100)}/{int((1-w)*100)} 00631L:{leg_name}':<32}"
                     f"{r['final']:>12.0f}{_pct(r['xirr']):>9}{_pct(vs_a):>9}"
                     f"{_pct(r['max_dd']):>10}{r['calmar']:>9.2f}{r['sells']:>7}{mark}")
    L.append("  解讀：★=Calmar 高於 A 純抱(風險調整後勝出)。0050 腿本身會漲，理應勝過現金腿。")
    L.append("=" * 88)
    return "\n".join(L)


def run_experiments(frame, signals, cfg) -> str:
    dates = frame.index
    target = frame["target"].to_numpy(dtype=np.float64)
    cheap = signals["cheapness"].to_numpy(dtype=np.float64)
    choppy = signals["is_choppy"].to_numpy(dtype=bool)
    Min = cfg["capital"]["monthly_input"]
    tiers = cfg["signal"]["add_tiers"]
    sm = int(cfg["capital"]["stale_deploy_months"])
    sf = cfg["capital"]["stale_deploy_frac"]
    base_cap = cfg["capital"]["pool_cap_months"] * Min * (1 - cfg["capital"]["base_ratio"])

    def dyn_base(i):
        sc = cheap[i]
        if np.isnan(sc):
            return 0.6
        return float(np.clip(0.3 + 0.6 * sc, 0.3, 0.9))

    variants = [
        ("A 純DCA (br=1.0)", None, base_cap, 0.70),
        ("S 現行 (br=0.6,cap=48k,thr=0.7)", lambda i: 0.6, base_cap, 0.70),
        ("S 動態拆分 (br=0.3~0.9)", dyn_base, base_cap, 0.70),
        ("S 集中彈藥 (br=0.6,池∞,thr=0.8)", lambda i: 0.6, INF_CAP, 0.80),
        ("S 集中彈藥2 (br=0.5,池∞,thr=0.7)", lambda i: 0.5, INF_CAP, 0.70),
    ]

    L = ["", "=" * 78, "優化實驗（只買不賣框架內）", "=" * 78]
    res = {}
    a_final = None
    L.append(f"{'變體':<34}{'期末市值':>12}{'XIRR':>9}{'vs A':>9}{'平均成本':>10}{'最大回撤':>9}")
    L.append("-" * 83)
    for name, brfn, cap, thr in variants:
        r = _run_accum(dates, target, cheap, choppy, Min, brfn, cap, thr, tiers, sm, sf)
        res[name] = r
        if a_final is None:
            a_final = r["final"]
        vs_a = r["final"] / a_final - 1.0
        L.append(f"{name:<34}{r['final']:>12.0f}{_pct(r['xirr']):>9}"
                 f"{_pct(vs_a):>9}{r['avg_cost']:>10.4f}{_pct(r['max_dd']):>9}")
    L.append("  解讀：全期(2016-2026 大多頭)下，任何留現金變體仍輸 A；動態拆分/集中彈藥改善有限。")

    # regime evaluation — fresh re-run inside non-bull windows (pool starts empty)
    L.append("")
    L.append("【換 regime 評估：各區間內重新開始，S(現行) vs A 純DCA】")
    L.append("  （pool 在區間起點為空，純看「此環境下撿便宜買槓桿能否贏 DCA」）")
    windows = [
        ("2018 修正震盪", "2018-01-01", "2019-06-30"),
        ("2022 空頭(峰→谷→復原)", "2021-12-01", "2023-06-30"),
        ("2024下半~2025 高波動", "2024-07-01", "2025-12-31"),
    ]
    for label, s0, s1 in windows:
        mask = (dates >= s0) & (dates <= s1)
        if mask.sum() < 20:
            continue
        d = dates[mask]
        t = target[mask]
        c = cheap[mask]
        ch = choppy[mask]
        a = _run_accum(d, t, c, ch, Min, None, base_cap, 0.70, tiers, sm, sf)
        s = _run_accum(d, t, c, ch, Min, lambda i: 0.6, base_cap, 0.70, tiers, sm, sf)
        a_ret = a["final"] / a["invested"] - 1.0
        s_ret = s["final"] / s["invested"] - 1.0
        verdict = "S 勝" if s_ret > a_ret else "A 勝"
        L.append(f"  {label:<24} A報酬 {_pct(a_ret):>9}  S報酬 {_pct(s_ret):>9}  "
                 f"差 {_pct(s_ret - a_ret):>9}  → {verdict}")
    L.append("  解讀：若 S 在陰跌/震盪區間贏 A，代表策略確實對槓桿衰減有效，只是被多頭樣本淹沒。")
    L.append("=" * 78)
    return "\n".join(L)
