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


def _run_rebalance_dyn(dates, target, safe_price, w_series, M_in, band):
    """Rebalance to a per-day target weight w_series[i] (signal-driven). Same
    band/contribution logic as _run_rebalance but the target itself moves with
    cheapness: fear/cheap → higher 00631L target, greed → lower."""
    n = len(dates)
    ms = _month_starts(dates)
    units_a = units_b = 0.0
    value = np.zeros(n)
    cf = []
    buys = sells = 0
    for i in range(n):
        pa = target[i]
        pb = safe_price[i]
        wt = w_series[i]
        if ms[i]:
            units_b += M_in / pb
            cf.append((dates[i], -M_in))
        va = units_a * pa
        vb = units_b * pb
        total = va + vb
        if total > 0:
            w = va / total
            if ms[i] or abs(w - wt) > band:
                delta = total * wt - va
                units_a += delta / pa
                units_b -= delta / pb
                if delta > 0:
                    buys += 1
                elif delta < 0:
                    sells += 1
        value[i] = units_a * pa + units_b * pb
    cf.append((dates[-1], value[-1]))
    xirr = M.xirr([d for d, _ in cf], [a for _, a in cf])
    mdd = M.max_drawdown(value)
    return {
        "final": float(value[-1]),
        "xirr": xirr,
        "max_dd": mdd,
        "calmar": (xirr / abs(mdd)) if (xirr is not None and mdd < 0) else float("nan"),
        "buys": buys, "sells": sells,
    }


def run_signal_rebalance_experiments(frame, signals, cfg) -> str:
    """訊號驅動動態權重再平衡：目標權重隨 cheapness_score 浮動（恐懼買重/貪婪賣多）。
    與固定 50/50 對照，看擇時是否真的勝過純比例。"""
    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    cash_leg = np.ones(n)
    leg0050 = frame["c_close"].to_numpy(dtype=np.float64)
    cheap = signals["cheapness"].to_numpy(dtype=np.float64)
    Min = cfg["capital"]["monthly_input"]
    band = 0.05

    score = np.where(np.isnan(cheap), 0.5, cheap)

    def wseries(span, lo, hi):
        return np.clip(0.5 + (score - 0.5) * span, lo, hi)

    L = ["", "=" * 90, "訊號驅動動態權重再平衡（目標權重隨恐懼貪婪浮動，含賣出）", "=" * 90]
    a = _run_accum(dates, target, np.zeros(n), np.zeros(n, bool),
                   Min, None, INF_CAP, 1.0, cfg["signal"]["add_tiers"], 999, 0.0)
    a_calmar = (a["xirr"] / abs(a["max_dd"])) if a["max_dd"] < 0 else float("nan")
    fix = _run_rebalance(dates, target, cash_leg, Min, 0.50, band)
    fix_calmar = fix["calmar"]

    L.append(f"  基準: A純抱 Calmar {a_calmar:.2f} / 回撤 {_pct(a['max_dd'])}　|　"
             f"固定50/50現金 Calmar {fix_calmar:.2f} / 回撤 {_pct(fix['max_dd'])}")
    L.append(f"{'配置':<34}{'期末市值':>12}{'XIRR':>9}{'最大回撤':>10}{'Calmar':>9}{'買/賣':>11}")
    L.append("-" * 86)
    for label, leg, span in [
        ("動態 span0.4 現金 [0.3,0.7]", cash_leg, 0.4),
        ("動態 span0.8 現金 [0.3,0.7]", cash_leg, 0.8),
        ("動態 span0.8 0050 [0.3,0.7]", leg0050, 0.8),
    ]:
        ws = wseries(span, 0.3, 0.7)
        r = _run_rebalance_dyn(dates, target, leg, ws, Min, band)
        mark = ""
        if not np.isnan(r["calmar"]):
            if r["calmar"] > fix_calmar:
                mark += " >固定"
            if r["calmar"] > a_calmar:
                mark += " >純抱"
        bs = f"{r['buys']}/{r['sells']}"
        L.append(f"{label:<34}{r['final']:>12.0f}{_pct(r['xirr']):>9}"
                 f"{_pct(r['max_dd']):>10}{r['calmar']:>9.2f}{bs:>11}{mark}")
    L.append(f"  平均動態目標權重 span0.4={wseries(0.4,0.3,0.7).mean()*100:.1f}%  "
             f"span0.8={wseries(0.8,0.3,0.7).mean()*100:.1f}%（區間 30~70%）")
    L.append("  解讀：>固定=擇時贏過純比例；>純抱=風險調整後勝 DCA。若都沒有，代表恐懼貪婪擇時在此樣本無增值。")
    L.append("=" * 90)
    return "\n".join(L)


def _causal_median(x, window=252, min_p=60):
    """Trailing median (no look-ahead) for the vol reference level."""
    n = len(x)
    out = np.full(n, np.nan)
    for i in range(n):
        s = max(0, i - window + 1)
        w = x[s:i + 1]
        w = w[~np.isnan(w)]
        if len(w) >= min(min_p, i + 1) and len(w) > 0:
            out[i] = np.median(w)
    return out


def run_voltarget_experiments(frame, signals, cfg) -> str:
    """波動率目標再平衡：目標權重 ∝ 1/已實現波動（高波動降槓桿）。失敗的恐懼版的鏡像。"""
    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    cash_leg = np.ones(n)
    leg0050 = frame["c_close"].to_numpy(dtype=np.float64)
    rv = signals["realized_vol"].to_numpy(dtype=np.float64)
    Min = cfg["capital"]["monthly_input"]
    band = 0.05

    vref = _causal_median(rv, 252, 60)

    def wseries(lo, hi):
        with np.errstate(divide="ignore", invalid="ignore"):
            w = np.where((rv > 0) & ~np.isnan(rv) & ~np.isnan(vref),
                         0.5 * vref / rv, 0.5)
        return np.clip(w, lo, hi)

    def cal(r):
        return r["calmar"]

    L = ["", "=" * 92, "波動率目標再平衡（高波動降槓桿，恐懼版的反向）", "=" * 92]
    a = _run_accum(dates, target, np.zeros(n), np.zeros(n, bool), Min, None,
                   INF_CAP, 1.0, cfg["signal"]["add_tiers"], 999, 0.0)
    a_cal = (a["xirr"] / abs(a["max_dd"])) if a["max_dd"] < 0 else float("nan")
    fix = _run_rebalance(dates, target, cash_leg, Min, 0.50, band)
    L.append(f"  基準: A純抱 Calmar {a_cal:.2f}/回撤{_pct(a['max_dd'])}  |  "
             f"固定50/50現金 Calmar {fix['calmar']:.2f}/回撤{_pct(fix['max_dd'])}/XIRR{_pct(fix['xirr'])}")
    L.append(f"{'配置':<32}{'XIRR':>9}{'最大回撤':>10}{'Calmar':>9}{'平均權重':>10}{'買/賣':>11}")
    L.append("-" * 84)
    for label, leg, lo, hi in [
        ("波動目標 現金 [0.3,0.7]", cash_leg, 0.3, 0.7),
        ("波動目標 現金 [0.2,0.8]", cash_leg, 0.2, 0.8),
        ("波動目標 0050 [0.3,0.7]", leg0050, 0.3, 0.7),
    ]:
        ws = wseries(lo, hi)
        r = _run_rebalance_dyn(dates, target, leg, ws, Min, band)
        mark = ""
        if not np.isnan(r["calmar"]):
            if r["calmar"] > fix["calmar"]:
                mark += " >固定"
            if r["calmar"] > a_cal:
                mark += " >純抱"
        bs = f"{r['buys']}/{r['sells']}"
        L.append(f"{label:<32}{_pct(r['xirr']):>9}{_pct(r['max_dd']):>10}"
                 f"{r['calmar']:>9.2f}{ws[~np.isnan(ws)].mean()*100:>9.1f}%{bs:>11}{mark}")
    L.append("  解讀：高波動時主動降槓桿,理論上壓低回撤。但已實現波動落後,可能在跌完才降(賣在低點)。")
    L.append("=" * 92)
    return "\n".join(L)


def _run_rb_cost(dates, target, safe_price, w_series, M_in, band, c_buy, c_sell):
    """Rebalance engine with transaction costs. w_series per-day target weight
    (scalar broadcast ok). c_buy/c_sell = fractional cost on each side."""
    n = len(dates)
    ms = _month_starts(dates)
    w_arr = w_series if hasattr(w_series, "__len__") else np.full(n, w_series)
    units_a = units_b = 0.0
    value = np.zeros(n)
    cf = []
    cost_paid = turnover = 0.0
    for i in range(n):
        pa = target[i]
        pb = safe_price[i]
        wt = w_arr[i]
        if ms[i]:
            units_b += M_in / pb
            cf.append((dates[i], -M_in))
        va = units_a * pa
        vb = units_b * pb
        total = va + vb
        if total > 0:
            w = va / total
            if ms[i] or abs(w - wt) > band:
                delta = total * wt - va
                if delta > 0:
                    units_a += delta * (1 - c_buy) / pa
                    units_b -= delta / pb
                    cost_paid += delta * c_buy
                    turnover += delta
                elif delta < 0:
                    units_a += delta / pa
                    units_b -= delta * (1 - c_sell) / pb
                    cost_paid += -delta * c_sell
                    turnover += -delta
        value[i] = units_a * pa + units_b * pb
    cf.append((dates[-1], value[-1]))
    xirr = M.xirr([d for d, _ in cf], [a for _, a in cf])
    mdd = M.max_drawdown(value)
    return {
        "final": float(value[-1]), "xirr": xirr, "max_dd": mdd,
        "calmar": (xirr / abs(mdd)) if (xirr is not None and mdd < 0) else float("nan"),
        "cost": cost_paid, "turnover": turnover,
        "invested": float(-sum(a for _, a in cf[:-1])),
    }


def run_cost_experiments(frame, signals, cfg) -> str:
    """加交易成本(台股 ETF:買0.1425%,賣0.1425%+稅0.1%)後重測,看波動目標優勢剩多少。"""
    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    cash = np.ones(n)
    rv = signals["realized_vol"].to_numpy(dtype=np.float64)
    Min = cfg["capital"]["monthly_input"]
    band = 0.05
    C_BUY, C_SELL = 0.001425, 0.002425

    vref = _causal_median(rv, 252, 60)
    with np.errstate(divide="ignore", invalid="ignore"):
        wvt = np.clip(np.where((rv > 0) & ~np.isnan(rv) & ~np.isnan(vref),
                               0.5 * vref / rv, 0.5), 0.3, 0.7)

    configs = [
        ("A 純抱 00631L", np.ones(n)),
        ("固定 50/50 現金", np.full(n, 0.5)),
        ("波動目標 現金[0.3,0.7]", wvt),
    ]
    L = ["", "=" * 96, "加交易成本後重測（買 0.1425% / 賣 0.1425%+稅 0.1%）", "=" * 96]
    L.append(f"{'配置':<26}{'XIRR(無成本)':>13}{'XIRR(含成本)':>13}{'回撤(含)':>10}"
             f"{'Calmar(含)':>11}{'總成本':>11}{'成本/投入':>10}")
    L.append("-" * 94)
    for label, w in configs:
        r0 = _run_rb_cost(dates, target, cash, w, Min, band, 0.0, 0.0)
        r1 = _run_rb_cost(dates, target, cash, w, Min, band, C_BUY, C_SELL)
        cost_ratio = r1["cost"] / r1["invested"]
        L.append(f"{label:<26}{_pct(r0['xirr']):>13}{_pct(r1['xirr']):>13}"
                 f"{_pct(r1['max_dd']):>10}{r1['calmar']:>11.2f}{r1['cost']:>11.0f}{_pct(cost_ratio):>10}")
    L.append("  解讀：A 純抱幾乎只買不賣→成本極低;波動目標換手高→成本侵蝕較多,看 Calmar(含成本)是否仍 >固定/純抱。")
    L.append("=" * 96)
    return "\n".join(L)


def run_hybrid_experiments(frame, signals, cfg) -> str:
    """測「高持股 + 小現金池(訊號部署,永不賣)」vs「固定永久現金緩衝」的取捨。
    回答:能否逼近純抱報酬又把 -54% 回撤壓到可承受。"""
    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    cash_leg = np.ones(n)
    cheap = signals["cheapness"].to_numpy(dtype=np.float64)
    choppy = signals["is_choppy"].to_numpy(dtype=bool)
    Min = cfg["capital"]["monthly_input"]
    tiers = cfg["signal"]["add_tiers"]
    sm = int(cfg["capital"]["stale_deploy_months"])
    sf = cfg["capital"]["stale_deploy_frac"]
    thr = cfg["signal"]["trigger_threshold"]
    cap_m = cfg["capital"]["pool_cap_months"]

    def cal(r):
        return (r["xirr"] / abs(r["max_dd"])) if (r["xirr"] is not None and r["max_dd"] < 0) else float("nan")

    a = _run_accum(dates, target, np.zeros(n), np.zeros(n, bool), Min, None,
                   INF_CAP, 1.0, tiers, 999, 0.0)
    a_cal = cal(a)

    L = ["", "=" * 92, "混合方案：高持股現金池(永不賣) vs 固定現金緩衝 — 我的建議驗證", "=" * 92]
    L.append(f"  基準 A 純抱: XIRR {_pct(a['xirr'])}  回撤 {_pct(a['max_dd'])}  Calmar {a_cal:.2f}")
    L.append(f"{'方案':<32}{'XIRR':>9}{'vs A報酬':>10}{'最大回撤':>10}{'Calmar':>9}{'說明':>8}")
    L.append("-" * 88)

    # high-base signal pool (never sells; only the small reserve is deployed on signal)
    for br in [0.85, 0.90]:
        cap = cap_m * Min * (1 - br)
        r = _run_accum(dates, target, cheap, choppy, Min, (lambda b: (lambda i: b))(br),
                       cap, thr, tiers, sm, sf)
        L.append(f"{f'現金池 base{br}(訊號部署,永不賣)':<32}{_pct(r['xirr']):>9}"
                 f"{_pct(r['final']/a['final']-1):>10}{_pct(r['max_dd']):>10}{cal(r):>9.2f}{'高持股':>8}")

    # permanent cash buffer via fixed rebalance (two-way, trims winners)
    for w in [0.85, 0.80, 0.70]:
        r = _run_rebalance(dates, target, cash_leg, Min, w, 0.05)
        L.append(f"{f'固定再平衡 {int(w*100)}/{int((1-w)*100)} 現金':<32}{_pct(r['xirr']):>9}"
                 f"{_pct(r['final']/a['final']-1):>10}{_pct(r['max_dd']):>10}{r['calmar']:>9.2f}{'永久緩衝':>8}")

    L.append("  解讀：高持股現金池幾乎=純抱(回撤不降);固定緩衝降回撤但砍報酬。看哪個取捨你接受。")
    L.append("=" * 92)
    return "\n".join(L)


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
