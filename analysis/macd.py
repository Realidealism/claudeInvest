"""
Multi-timeframe MACD / OSC system — port from CalculateTrade2.go (lines
16317-16498).

Three timeframes (Short / Medium / Long) — each computed by the Go
formulas, NOT the standard 12/26/9 MACD:

  ShortDIF  = (EMA3  + EMA8 ) / 2 - EMA21
  MediumDIF = (EMA5  + EMA13) / 2 - EMA34
  LongDIF   = (EMA8  + EMA21) / 2 - EMA55

  ShortDEM  = (EMA(ShortDIF, 3)  + EMA(ShortDIF, 8) ) / 2
  MediumDEM = (EMA(MediumDIF, 5) + EMA(MediumDIF, 13)) / 2
  LongDEM   = (EMA(LongDIF, 8)   + EMA(LongDIF, 21)) / 2

  OSC = DIF - DEM   (per timeframe)

Derived flags per timeframe:
  osc_x         = OSC[t] - OSC[t-1]
  osc_direction = osc_x >= 0                 # 動能方向
  osc_status    = osc_x > osc_x[t-1]         # 動能加速
  osc_status_up   = direction AND osc_x > osc_x[t-1] * 0.97
  osc_status_down = !direction AND osc_x < osc_x[t-1] * 0.97

  macd_death_gold = OSC > 0                  # in golden state
  macd_death      = yesterday gold AND today not (cross to death)
  macd_gold       = today gold AND yesterday not (cross to gold)

  macd_convergence_pte = death_gold AND !direction
                         (in gold state but momentum weakening — top divergence)
  macd_convergence_nte = !death_gold AND direction
                         (in death state but momentum strengthening — bottom)

Plus Short OSC rolling extremes at 2/3/5/8/13/21/34 days (Big and Small).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray

from analysis.indicators import ema, rolling_highest, rolling_lowest

F32 = np.float32
F32Array = NDArray[np.float32]
BoolArray = NDArray[np.bool_]


@dataclass
class MACDOneScope:
    """One timeframe (short / medium / long) MACD result."""
    dif: F32Array
    dem: F32Array
    osc: F32Array

    osc_x: F32Array
    osc_direction: BoolArray
    osc_status: BoolArray
    osc_status_up: BoolArray
    osc_status_down: BoolArray

    macd_death_gold: BoolArray
    macd_death: BoolArray
    macd_gold: BoolArray
    macd_convergence_pte: BoolArray
    macd_convergence_nte: BoolArray


@dataclass
class MACDResult:
    """Full multi-timeframe MACD result."""
    short:  MACDOneScope
    medium: MACDOneScope
    long:   MACDOneScope

    # Short OSC rolling extremes (Go references heavily)
    short_osc_b: dict[int, F32Array] = field(default_factory=dict)
    short_osc_s: dict[int, F32Array] = field(default_factory=dict)


_OSC_B_PERIODS = (2, 3, 5, 8, 13, 21, 34)
_OSC_S_PERIODS = (2, 3, 5, 8, 13, 21, 34)


def _diff1(arr: F32Array) -> F32Array:
    """Today's value minus yesterday's; first bar = 0."""
    out = np.zeros_like(arr)
    out[1:] = arr[1:] - arr[:-1]
    return out


def _shift1(arr: np.ndarray) -> np.ndarray:
    out = np.empty_like(arr)
    out[0] = arr[0]
    out[1:] = arr[:-1]
    return out


def _build_scope(dif: F32Array, dem: F32Array) -> MACDOneScope:
    osc = (dif - dem).astype(F32)
    osc_x = _diff1(osc)
    osc_x_prev = _shift1(osc_x)

    direction = osc_x >= 0
    status = osc_x > osc_x_prev
    status_up = direction & (osc_x > osc_x_prev * 0.97)
    status_down = (~direction) & (osc_x < osc_x_prev * 0.97)

    death_gold = osc > 0
    prev_death_gold = _shift1(death_gold)
    macd_death = prev_death_gold & (~death_gold)
    macd_gold = death_gold & (~prev_death_gold)

    convergence_pte = death_gold & (~direction)
    convergence_nte = (~death_gold) & direction

    return MACDOneScope(
        dif=dif.astype(F32),
        dem=dem.astype(F32),
        osc=osc,
        osc_x=osc_x.astype(F32),
        osc_direction=direction,
        osc_status=status,
        osc_status_up=status_up,
        osc_status_down=status_down,
        macd_death_gold=death_gold,
        macd_death=macd_death,
        macd_gold=macd_gold,
        macd_convergence_pte=convergence_pte,
        macd_convergence_nte=convergence_nte,
    )


def calculate_macd(close: F32Array) -> MACDResult:
    """Compute Short / Medium / Long MACD per Go formulas."""
    close = close.astype(F32)

    # Base EMAs of close
    ema3  = ema(close, 3)
    ema5  = ema(close, 5)
    ema8  = ema(close, 8)
    ema13 = ema(close, 13)
    ema21 = ema(close, 21)
    ema34 = ema(close, 34)
    ema55 = ema(close, 55)

    # DIF (composite of fast EMAs minus slow EMA)
    short_dif  = ((ema3  + ema8 ) / 2 - ema21).astype(F32)
    medium_dif = ((ema5  + ema13) / 2 - ema34).astype(F32)
    long_dif   = ((ema8  + ema21) / 2 - ema55).astype(F32)

    # DEM (EMA of DIF, two periods averaged)
    short_dem  = ((ema(short_dif, 3)  + ema(short_dif, 8) ) / 2).astype(F32)
    medium_dem = ((ema(medium_dif, 5) + ema(medium_dif, 13)) / 2).astype(F32)
    long_dem   = ((ema(long_dif, 8)   + ema(long_dif, 21)) / 2).astype(F32)

    short  = _build_scope(short_dif,  short_dem)
    medium = _build_scope(medium_dif, medium_dem)
    long_  = _build_scope(long_dif,   long_dem)

    short_osc_b = {p: rolling_highest(short.osc, p) for p in _OSC_B_PERIODS}
    short_osc_s = {p: rolling_lowest (short.osc, p) for p in _OSC_S_PERIODS}

    return MACDResult(
        short=short, medium=medium, long=long_,
        short_osc_b=short_osc_b, short_osc_s=short_osc_s,
    )
