"""
Shared constants for technical analysis modules.

Fibonacci-based periods are used throughout all indicator calculations.
"""

# ── Period Groups ───────────────────────────────────────────────────────────

# Standard SMA periods (close, volume, etc.)
SMA_PERIODS = (3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377)
SMA_PERIODS_SHORT = (3, 5, 8, 13, 21, 34, 55)  # also used for EMA / pre-SMA

# Bollinger band periods
BOLL_PERIODS = (21, 34, 55)

# Rolling high/low (BS) periods
BS_PERIODS = (2, 3, 5, 8, 13, 21, 34, 55, 89)

# ── Trend Sort Definitions ──────────────────────────────────────────────────
# Each entry: (short_period, mid_period, long_period)

SORT_NORMAL = {
    "short": (3, 8, 21),
    "medium": (5, 13, 34),
    "long": (8, 21, 55),
}

SORT_LP = {
    "short": (21, 55, 144),
    "medium": (34, 89, 233),
    "long": (55, 144, 377),
}

# ── Turn Point (扣抵) Config ────────────────────────────────────────────────
# (ma_period, lookback_offset, bs_period)
# period 3/5: direct close-to-close comparison (bs_period=None)
TURN_CONFIGS = [
    (3, 2, None),
    (5, 4, None),
    (8, 6, 2),
    (13, 10, 3),
    (21, 16, 5),
    (34, 26, 8),
    (55, 42, 13),
    (89, 68, 21),
    (144, 110, 34),
    (233, 178, 55),
    (377, 288, 89),
]

# ── Value Level Thresholds ──────────────────────────────────────────────────
# Classification based on 8-day SMA, yielding level 0–15
VALUE_THRESHOLDS = [
    10, 20, 30, 50, 75, 100, 200, 300, 500, 750, 1000, 2000, 3000, 5000, 7500,
]

# ── Knot (均線糾結) Definitions ─────────────────────────────────────────────
# (label, ma_periods, rolling_window_for_std_and_ma)
# Original defs: all timeframes include short MAs for "full-spectrum resonance"
KNOT_DEFS = [
    ("short", (3, 5, 8), 55),
    ("medium", (3, 5, 8, 13, 21), 144),
    ("long", (3, 5, 8, 13, 21, 34, 55), 377),
]
# Pure defs: only the MAs native to each timeframe
KNOT_PURE_DEFS = [
    ("medium_pure", (13, 21, 34), 144),
    ("long_pure", (34, 55, 89), 377),
]

# ── Volume ──────────────────────────────────────────────────────────────────

VOLUME_SMA_PERIODS = (3, 5, 8, 13, 21, 34, 55)

# Rolling max periods for volume
VOLUME_HIGH_PERIODS = (2, 3, 5, 8, 13, 21, 34, 55)
# Rolling min periods for volume
VOLUME_LOW_PERIODS = (5, 8, 13, 55)

# Rolling max periods for volume diff (VX)
VX_HIGH_PERIODS = (3, 5, 8, 13, 21, 34, 55)

# Volume level thresholds (based on 8-day volume SMA), yielding level 0–11
VOLUME_LEVEL_THRESHOLDS = [
    100_000, 300_000, 900_000, 2_700_000, 8_100_000, 24_300_000,
    72_900_000, 218_700_000, 656_100_000, 1_968_300_000, 5_904_900_000,
]

# No-deal detection: (window, threshold)
NO_DEAL_CONFIGS = [
    (21, 1),
    (34, 2),
    (55, 3),
]

# Volume extreme groups: (label, ma_periods) — max/min of these SMA values
VOLUME_EXTREME_GROUPS = {
    "short": (8, 5, 3),
    "medium": (21, 13, 8),
    "long": (55, 34, 21),
}

# ── Money (成交金額) ────────────────────────────────────────────────────────

MONEY_LEVEL_THRESHOLDS = [
    1_000_000, 3_000_000, 9_000_000, 27_000_000, 81_000_000, 243_000_000,
    729_000_000, 2_187_000_000, 6_561_000_000, 19_683_000_000, 59_049_000_000,
]

# Fish/Dead rolling count configs: (window, threshold)
FISH_DEAD_SHORT_CONFIGS = [(3, 2), (5, 3), (8, 5), (13, 8)]
FISH_DEAD_LONG_CONFIGS = [(13, 5), (21, 8), (34, 13), (55, 21)]
