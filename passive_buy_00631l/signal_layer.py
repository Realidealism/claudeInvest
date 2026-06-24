"""Signal layer (M1): build cheapness_score in [0,1] from the TAIEX proxy +
whole-market sentiment. Higher score = cheaper = stronger reason to deploy.

Never touches the 00631L price (avoids leverage path-dependency in the signal).
Each raw factor is mapped to a "cheap value" (higher = cheaper) then converted to
a rolling percentile so dimensions are comparable and self-relative.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from analysis.indicators import sma, rolling_std, rolling_highest


def _rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder RSI (0-100). Partial window for early bars."""
    n = len(close)
    out = np.full(n, 50.0)
    if n < 2:
        return out
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_g = avg_l = 0.0
    for i in range(1, n):
        g, l = gain[i - 1], loss[i - 1]
        if i <= period:
            avg_g += g / period
            avg_l += l / period
        else:
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period
        rs = avg_g / avg_l if avg_l > 0 else (np.inf if avg_g > 0 else 0.0)
        out[i] = 100.0 - 100.0 / (1.0 + rs)
    return out


def _percent_b(close: np.ndarray, period: int = 20, mult: float = 2.0) -> np.ndarray:
    """Bollinger %B: 0 = at lower band, 1 = at upper band."""
    c32 = close.astype(np.float32)
    mid = sma(c32, period).astype(np.float64)
    sd = rolling_std(c32, period).astype(np.float64)
    upper = mid + mult * sd
    lower = mid - mult * sd
    width = upper - lower
    with np.errstate(divide="ignore", invalid="ignore"):
        pb = np.where(width > 0, (close - lower) / width, 0.5)
    return pb


def _rolling_percentile(cv: np.ndarray, window: int, min_periods: int) -> np.ndarray:
    """For each i, fraction of the trailing window <= cv[i], in (0,1].

    Uses the available (expanding) window until `min_periods` filled; NaN-safe.
    """
    n = len(cv)
    out = np.full(n, np.nan)
    for i in range(n):
        if np.isnan(cv[i]):
            continue
        start = max(0, i - window + 1)
        win = cv[start : i + 1]
        win = win[~np.isnan(win)]
        if len(win) < min_periods:
            if i + 1 < min_periods:
                continue
            # early bars: use whatever we have once we pass a small floor
            if len(win) < min(min_periods, 20):
                continue
        out[i] = float(np.mean(win <= cv[i]))
    return out


def _annualized_vol(ret: np.ndarray, period: int = 20) -> np.ndarray:
    r32 = ret.astype(np.float32)
    sd = rolling_std(r32, period).astype(np.float64)
    return sd * np.sqrt(252.0)


def build_signals(frame: pd.DataFrame, cfg) -> pd.DataFrame:
    """Return a frame (same index) with technical, market, cheapness, is_choppy,
    realized_vol, drawdown and the component sub-scores."""
    sig = cfg["signal"]
    window = int(sig["lookback_years"] * 252)
    min_p = int(sig["min_periods"])

    proxy = frame["proxy"].to_numpy(dtype=np.float64)
    out = pd.DataFrame(index=frame.index)

    # ── technical "cheap values" (higher = cheaper), on TAIEX ────────────────
    p32 = proxy.astype(np.float32)
    bias20 = -(proxy - sma(p32, 20).astype(np.float64))
    bias60 = -(proxy - sma(p32, 60).astype(np.float64))
    roll_hi = rolling_highest(p32, 240).astype(np.float64)
    drawdown = proxy / np.where(roll_hi > 0, roll_hi, np.nan) - 1.0  # negative
    ma240 = sma(p32, 240).astype(np.float64)
    dist_year = -(proxy / np.where(ma240 > 0, ma240, np.nan) - 1.0)  # below year-line = cheap
    rsi = _rsi(proxy, 14)
    pb = _percent_b(proxy, 20)

    tech_factors = {
        "f_bias20": bias20,
        "f_bias60": bias60,
        "f_drawdown": -drawdown,   # deeper drawdown = cheaper
        "f_dist_year": dist_year,
        "f_rsi": -rsi,             # lower RSI = cheaper
        "f_pctb": -pb,             # lower %B = cheaper
    }
    tech_pcts = []
    for name, cv in tech_factors.items():
        pct = _rolling_percentile(cv, window, min_p)
        out[name] = pct
        tech_pcts.append(pct)
    with warnings.catch_warnings():  # early warmup bars are all-NaN → empty slice
        warnings.simplefilter("ignore", RuntimeWarning)
        technical = np.nanmean(np.vstack(tech_pcts), axis=0)

    # ── market "fear = cheap" factors (whole-market) ─────────────────────────
    # breadth net (medium), weak breadth = fear = cheap
    mu = frame.get("medium_up")
    md = frame.get("medium_down")
    if mu is not None and md is not None:
        mu = mu.to_numpy(dtype=np.float64)
        md = md.to_numpy(dtype=np.float64)
        denom = np.where((mu + md) > 0, mu + md, np.nan)
        breadth_net = (mu - md) / denom
        cv_breadth = -breadth_net
    else:
        cv_breadth = np.full(len(proxy), np.nan)

    # limit-down imbalance: more 跌停 than 漲停 = panic = cheap
    al = frame.get("advance_limit")
    dl = frame.get("decline_limit")
    if al is not None and dl is not None:
        al = al.to_numpy(dtype=np.float64)
        dl = dl.to_numpy(dtype=np.float64)
        cv_limit = dl - al
    else:
        cv_limit = np.full(len(proxy), np.nan)

    # realized volatility: high vol = fear = cheap
    ret = np.zeros(len(proxy))
    ret[1:] = proxy[1:] / proxy[:-1] - 1.0
    realized_vol = _annualized_vol(ret, 20)
    cv_vol = realized_vol.copy()

    # margin balance falling vs 20d ago = deleveraging fear = cheap
    mb = frame.get("margin_balance")
    if mb is not None:
        mb = mb.to_numpy(dtype=np.float64)
        mb_chg = np.full(len(mb), np.nan)
        mb_chg[20:] = mb[20:] / np.where(mb[:-20] > 0, mb[:-20], np.nan) - 1.0
        cv_margin = -mb_chg
    else:
        cv_margin = np.full(len(proxy), np.nan)

    market_factors = {
        "m_breadth": cv_breadth,
        "m_limit": cv_limit,
        "m_vol": cv_vol,
        "m_margin": cv_margin,
    }
    mkt_pcts = []
    for name, cv in market_factors.items():
        pct = _rolling_percentile(cv, window, min_p)
        out[name] = pct
        mkt_pcts.append(pct)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        market = np.nanmean(np.vstack(mkt_pcts), axis=0)

    w = sig["dim_weights"]
    cheapness = w["technical"] * technical + w["market"] * market

    out["technical"] = technical
    out["market"] = market
    out["cheapness"] = cheapness
    out["realized_vol"] = realized_vol
    out["drawdown"] = drawdown

    # ── decay guard: high vol but shallow drawdown = choppy washout ──────────
    dg = sig["decay_guard"]
    if dg.get("enabled", True):
        vol_pct = _rolling_percentile(realized_vol, window, min_p)
        is_choppy = (vol_pct > dg["vol_high_pct"]) & (np.abs(drawdown) < dg["shallow_dd"])
        is_choppy = np.where(np.isnan(vol_pct) | np.isnan(drawdown), False, is_choppy)
    else:
        is_choppy = np.zeros(len(proxy), dtype=bool)
    out["is_choppy"] = is_choppy

    return out
