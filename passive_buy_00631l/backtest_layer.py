"""Backtest layer (M3): custom buy-and-accumulate loop, daily granularity,
monthly cashflows. Runs the 4 cohorts (SPEC §4.2) in one pass over the 00631L
trading calendar so date handling is identical.

Cohorts:
  A  純 DCA 00631L        — 每月全額 M 買進（標竿，策略須勝它）
  B  基礎額 only          — 每月只買 B，R 永久閒置（cash drag 對照）
  C  純 DCA 0050          — 價格報酬 + 近似總報酬雙軌
  S  Strategy             — 基礎額 + 現金池便宜訊號階梯部署
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .strategy_layer import CapitalParams, deploy_fraction, should_deploy


def _month_starts(index: pd.DatetimeIndex) -> np.ndarray:
    """True at the first trading day of each (year, month)."""
    ym = index.year * 100 + index.month
    flags = np.zeros(len(index), dtype=bool)
    flags[0] = True
    flags[1:] = ym[1:] != ym[:-1]
    return flags


def run_backtests(frame: pd.DataFrame, signals: pd.DataFrame, cfg) -> dict:
    cap = CapitalParams(
        monthly_input=cfg["capital"]["monthly_input"],
        base_ratio=cfg["capital"]["base_ratio"],
        pool_cap_months=cfg["capital"]["pool_cap_months"],
        stale_deploy_months=int(cfg["capital"]["stale_deploy_months"]),
        stale_deploy_frac=cfg["capital"]["stale_deploy_frac"],
    )
    tiers = cfg["signal"]["add_tiers"]
    threshold = cfg["signal"]["trigger_threshold"]

    dates = frame.index
    n = len(dates)
    target = frame["target"].to_numpy(dtype=np.float64)
    c_close = frame["c_close"].to_numpy(dtype=np.float64)
    cheap = signals["cheapness"].to_numpy(dtype=np.float64)
    choppy = signals["is_choppy"].to_numpy(dtype=bool)

    # 0050 approximate total-return series (reinvested dividends)
    div_yield = cfg["baseline_c"]["div_yield_annual"]
    tr_mult = np.cumprod(np.full(n, 1.0 + div_yield / 252.0))
    c_tr = c_close * tr_mult

    M, B, R = cap.monthly_input, cap.base_amount, cap.reserve_amount
    ms = _month_starts(dates)

    # ── per-cohort state ─────────────────────────────────────────────────────
    state = {
        "A": dict(units=0.0, cash=0.0, basis=0.0),
        "B": dict(units=0.0, cash=0.0, basis=0.0),   # cash = idle reserve
        "Cp": dict(units=0.0, cash=0.0, basis=0.0),  # 0050 price-return
        "Ct": dict(units=0.0, cash=0.0, basis=0.0),  # 0050 total-return
        "S": dict(units=0.0, cash=0.0, basis=0.0),   # cash = deployable pool
    }
    value = {k: np.zeros(n) for k in state}
    pool_series = np.zeros(n)           # Strategy cash-pool level over time
    cashflows = {k: [] for k in state}  # list of (date, amount)
    deploy_points = []                  # Strategy deploy events
    months_since_deploy = 0

    for i in range(n):
        px = target[i]
        if ms[i]:
            months_since_deploy += 1
            d = dates[i]
            # A: full M into target
            state["A"]["units"] += M / px
            state["A"]["basis"] += M
            cashflows["A"].append((d, -M))
            # B: only B into target, R idle
            state["B"]["units"] += B / px
            state["B"]["basis"] += B
            state["B"]["cash"] += R
            cashflows["B"].append((d, -M))
            # C price / total: full M into 0050
            if not np.isnan(c_close[i]) and c_close[i] > 0:
                state["Cp"]["units"] += M / c_close[i]
                state["Cp"]["basis"] += M
                state["Ct"]["units"] += M / c_tr[i]
                state["Ct"]["basis"] += M
            cashflows["Cp"].append((d, -M))
            cashflows["Ct"].append((d, -M))
            # S: base buy + reserve into pool (cap overflow buys target)
            state["S"]["units"] += B / px
            state["S"]["basis"] += B
            pool = state["S"]["cash"] + R
            if pool > cap.pool_cap:
                overflow = pool - cap.pool_cap
                state["S"]["units"] += overflow / px
                state["S"]["basis"] += overflow
                pool = cap.pool_cap
            state["S"]["cash"] = pool
            cashflows["S"].append((d, -M))
            # stale deploy: too long without a trigger → force-deploy part of pool
            if months_since_deploy >= cap.stale_deploy_months and state["S"]["cash"] > 0:
                amt = state["S"]["cash"] * cap.stale_deploy_frac
                state["S"]["units"] += amt / px
                state["S"]["basis"] += amt
                state["S"]["cash"] -= amt
                deploy_points.append((dates[i], px, float(cheap[i]) if not np.isnan(cheap[i]) else None, amt, "stale"))
                months_since_deploy = 0

        # S: daily tiered deployment on cheapness signal
        sc = cheap[i]
        if state["S"]["cash"] > 0 and not np.isnan(sc) and should_deploy(sc, bool(choppy[i]), threshold):
            frac = deploy_fraction(sc, tiers)
            amt = state["S"]["cash"] * frac
            if amt > 0:
                state["S"]["units"] += amt / px
                state["S"]["basis"] += amt
                state["S"]["cash"] -= amt
                deploy_points.append((dates[i], px, float(sc), amt, "signal"))
                months_since_deploy = 0

        value["A"][i] = state["A"]["units"] * px
        value["B"][i] = state["B"]["units"] * px + state["B"]["cash"]
        value["Cp"][i] = state["Cp"]["units"] * c_close[i] if c_close[i] > 0 else value["Cp"][i - 1]
        value["Ct"][i] = state["Ct"]["units"] * c_tr[i]
        value["S"][i] = state["S"]["units"] * px + state["S"]["cash"]
        pool_series[i] = state["S"]["cash"]

    # append terminal value as positive cashflow for XIRR
    last_d = dates[-1]
    results = {}
    labels = {
        "A": "A 純DCA 00631L",
        "B": "B 基礎額only(R閒置)",
        "Cp": "C 純DCA 0050(價格)",
        "Ct": "C 純DCA 0050(總報酬近似)",
        "S": "Strategy 現金池加碼",
    }
    for k, st in state.items():
        cf = list(cashflows[k])
        cf.append((last_d, value[k][-1]))
        results[k] = {
            "label": labels[k],
            "value": value[k],
            "units": st["units"],
            "basis": st["basis"],
            "final_cash": st["cash"],
            "final_value": float(value[k][-1]),
            "invested": float(-sum(a for _, a in cashflows[k])),
            "cf_dates": [d for d, _ in cf],
            "cf_amounts": [a for _, a in cf],
        }
    results["_meta"] = {
        "dates": dates,
        "deploy_points": deploy_points,
        "pool_series": pool_series,
        "n_months": int(ms.sum()),
    }
    return results
