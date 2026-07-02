"""Intraday full-day volume/value estimator using the h(t) curve.

h(t) is built from tw.intraday_value_profile data collected by the sweeper.
Each bucket's fraction is smoothed using three EMAs (21/34/55 day) averaged
together, giving more weight to recent trading days while staying stable.

Usage:
    from intraday.estimate import estimate_daily_volume, estimate_daily_value

    est_vol = estimate_daily_volume(current_volume=123000, current_time=now)
    est_val = estimate_daily_value(current_value=98000000, current_time=now)
"""

from __future__ import annotations

from datetime import date as _date, datetime, time as dtime, timedelta, timezone
from functools import lru_cache

from db.connection import get_cursor


_TPE_TZ = timezone(timedelta(hours=8))
_BUCKET_MINUTES = 5

# Session boundaries
_SESSION_OPEN = dtime(hour=9, minute=0)
_SESSION_CLOSE = dtime(hour=13, minute=30)

# Snapshot loop waits until the sweep has accumulated at least this many
# 5-minute buckets in today's intraday_value_profile before attempting the
# first pass. With only 1–2 buckets the h(t) projection is dominated by
# noise and produces wildly distorted full-day estimates.
_MIN_BUCKETS_FOR_SNAPSHOT = 3


class HCurveNotReadyError(RuntimeError):
    """h(t) is unusable right now — caller should sleep + retry, not crash.

    Distinct from a hard configuration error so the snapshot daemon can
    keep looping during the first 15 minutes of the session while the
    sweeper accumulates value-profile buckets."""


def _time_bucket(t: dtime) -> str:
    """Round up to the nearest 5-minute bucket, matching sweeper logic."""
    m = t.minute - (t.minute % _BUCKET_MINUTES) + _BUCKET_MINUTES
    h = t.hour
    if m >= 60:
        h += 1
        m -= 60
    return f"{h:02d}:{m:02d}"


def _all_buckets() -> list[str]:
    """Generate all 5-minute bucket labels from 09:05 to 13:30."""
    buckets = []
    h, m = 9, 5
    while (h, m) <= (13, 30):
        buckets.append(f"{h:02d}:{m:02d}")
        m += _BUCKET_MINUTES
        if m >= 60:
            h += 1
            m -= 60
    return buckets


_EMA_PERIODS = (21, 34, 55)
# Periods that should clamp to available history length when data is short,
# so EMA(34/55) don't stay seed-biased while we accumulate trading days.
_EMA_CLAMP_PERIODS = frozenset({34, 55})


def _ema(values: list[float], period: int) -> float:
    """Compute EMA over a chronologically ordered list, return the last value."""
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


def _effective_period(period: int, n: int) -> int:
    if period in _EMA_CLAMP_PERIODS and n < period:
        return max(n, 1)
    return period


def _fetch_bucket_data(lookback_days: int) -> tuple[list, dict[str, dict[str, int]]]:
    """Fetch recent complete trading days from intraday_value_profile.

    Returns (sorted_dates_oldest_first, by_date_buckets) where by_date_buckets
    is {trade_date: {time_bucket: cumulative_value}}. Only days with a '13:30'
    bucket are included.
    """
    with get_cursor() as cur:
        # Find eligible trading days (those with a '13:30' bucket)
        cur.execute(
            """
            SELECT DISTINCT trade_date
            FROM tw.intraday_value_profile
            WHERE time_bucket = '13:30'
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (lookback_days,),
        )
        dates = [row["trade_date"] for row in cur.fetchall()]

        if not dates:
            return [], {}

        # Fetch all profile rows for those dates
        cur.execute(
            """
            SELECT trade_date, time_bucket, market_total_value
            FROM tw.intraday_value_profile
            WHERE trade_date = ANY(%s)
            ORDER BY trade_date, time_bucket
            """,
            (dates,),
        )
        rows = cur.fetchall()

    # Group by date: {date: {bucket: value}}
    by_date: dict[str, dict[str, int]] = {}
    for row in rows:
        by_date.setdefault(row["trade_date"], {})[row["time_bucket"]] = row["market_total_value"]

    # Drop days that would distort the curve:
    #   - missing 09:05 (incomplete-start days, e.g. sweeper started late)
    #   - 09:05 value == 13:30 value (stale/holiday days, no real trading)
    for d in list(by_date.keys()):
        buckets = by_date[d]
        first = buckets.get("09:05")
        last = buckets.get("13:30")
        if first is None or last is None or first == last:
            del by_date[d]

    # Sort dates chronologically (oldest first) for EMA calculation
    return sorted(by_date.keys()), by_date


def today_bucket_count(trade_date: _date) -> int:
    """Return how many 5-minute buckets have been written for trade_date.

    The snapshot daemon uses this to decide whether the sweeper has
    accumulated enough warm-up data (>= _MIN_BUCKETS_FOR_SNAPSHOT) for
    today's h(t) projection to be stable enough to evaluate against.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM tw.intraday_value_profile
            WHERE trade_date = %s
            """,
            (trade_date,),
        )
        row = cur.fetchone()
    if row is None:
        return 0
    n = row["n"] if isinstance(row, dict) else row[0]
    return int(n or 0)


def require_today_buckets(trade_date: _date) -> int:
    """Raise HCurveNotReadyError if the sweeper hasn't accumulated enough
    today buckets yet. Returns the bucket count when OK."""
    n = today_bucket_count(trade_date)
    if n < _MIN_BUCKETS_FOR_SNAPSHOT:
        raise HCurveNotReadyError(
            f"sweeper has only {n} bucket(s) for {trade_date}; "
            f"need >= {_MIN_BUCKETS_FOR_SNAPSHOT} before snapshot can run."
        )
    return n


def get_h_curve(lookback_days: int = 80) -> dict[str, float]:
    """Build the h(t) curve from recent trading days.

    Returns a dict mapping time_bucket -> cumulative fraction (0.0 to 1.0).
    The last bucket ('13:30') is always 1.0 by definition.

    Each bucket's fraction is smoothed by averaging three EMAs (21/34/55)
    over the daily fraction series, giving recent days more weight.

    lookback_days defaults to 80 to provide enough warm-up data for the
    55-period EMA. Only complete trading days (with a '13:30' bucket) are used.
    """
    sorted_dates, by_date = _fetch_bucket_data(lookback_days)
    if not sorted_dates:
        return {}

    all_b = _all_buckets()
    # bucket -> list of fractions in chronological order
    bucket_fractions: dict[str, list[float]] = {b: [] for b in all_b}

    for d in sorted_dates:
        buckets = by_date[d]
        daily_total = buckets.get("13:30")
        if not daily_total or daily_total <= 0:
            continue
        for b in all_b:
            val = buckets.get(b)
            if val is not None:
                bucket_fractions[b].append(val / daily_total)

    # Average of 3 EMAs (21/34/55) for each bucket
    h_curve: dict[str, float] = {}
    for b in all_b:
        fracs = bucket_fractions[b]
        if fracs:
            n = len(fracs)
            ema_avg = sum(_ema(fracs, _effective_period(p, n)) for p in _EMA_PERIODS) / len(_EMA_PERIODS)
            h_curve[b] = ema_avg

    # Ensure the last bucket is exactly 1.0
    if "13:30" in h_curve:
        h_curve["13:30"] = 1.0

    return h_curve


def get_baseline_curve(lookback_days: int = 80) -> dict[str, float]:
    """Build EMA-averaged historical cumulative value at each bucket (in NTD).

    Unlike get_h_curve which returns fractions, this returns the absolute
    market-wide total_value typically seen at that bucket. Used by
    rvol_at_time to compare today's absolute level against history.
    """
    sorted_dates, by_date = _fetch_bucket_data(lookback_days)
    if not sorted_dates:
        return {}

    all_b = _all_buckets()
    bucket_values: dict[str, list[float]] = {b: [] for b in all_b}

    for d in sorted_dates:
        buckets = by_date[d]
        for b in all_b:
            val = buckets.get(b)
            if val is not None:
                bucket_values[b].append(float(val))

    baseline: dict[str, float] = {}
    for b in all_b:
        vals = bucket_values[b]
        if vals:
            n = len(vals)
            ema_avg = sum(_ema(vals, _effective_period(p, n)) for p in _EMA_PERIODS) / len(_EMA_PERIODS)
            baseline[b] = ema_avg

    return baseline


def _get_h(current_time: datetime, h_curve: dict[str, float] | None = None) -> float | None:
    """Look up h(t) for the given time. Returns None if data is insufficient."""
    if h_curve is None:
        h_curve = get_h_curve()
    if not h_curve:
        return None

    t = current_time.astimezone(_TPE_TZ).time() if current_time.tzinfo else current_time.time()

    # Before market open
    if t < _SESSION_OPEN:
        return None

    # After market close
    if t >= dtime(hour=13, minute=30):
        return 1.0

    bucket = _time_bucket(t)
    return h_curve.get(bucket)


def estimate_daily_value(current_value: int | float, current_time: datetime,
                         h_curve: dict[str, float] | None = None) -> int | None:
    """Estimate full-day turnover from current cumulative value.

    Returns None if h(t) data is insufficient or current_time is outside
    the trading session.
    """
    h = _get_h(current_time, h_curve)
    if h is None or h <= 0.05:
        # h < 5% means we're in the first few minutes; estimate is unreliable
        return None
    return int(current_value / h)


def estimate_daily_volume(current_volume: int | float, current_time: datetime,
                          h_curve: dict[str, float] | None = None) -> int | None:
    """Estimate full-day volume from current cumulative volume.

    Uses the same h(t) curve built from market-wide turnover data.
    Returns None if data is insufficient.
    """
    h = _get_h(current_time, h_curve)
    if h is None or h <= 0.05:
        return None
    return int(current_volume / h)


def rvol_at_time(current_cumulative: int | float, current_time: datetime,
                 baseline_curve: dict[str, float] | None = None) -> float | None:
    """Ratio of today's cumulative market value to the historical baseline.

    RVOL = 1.0 means today is tracking exactly the historical norm at this
    time of day. RVOL = 2.0 means today has already done twice the typical
    cumulative turnover by now — a strong "abnormally active day" flag.

    Returns None if current_time is outside the trading session or the
    baseline curve has no data for this bucket.
    """
    if baseline_curve is None:
        baseline_curve = get_baseline_curve()
    if not baseline_curve:
        return None

    t = current_time.astimezone(_TPE_TZ).time() if current_time.tzinfo else current_time.time()

    if t < _SESSION_OPEN:
        return None
    if t >= _SESSION_CLOSE:
        bucket = "13:30"
    else:
        bucket = _time_bucket(t)

    baseline = baseline_curve.get(bucket)
    if baseline is None or baseline <= 0:
        return None
    return current_cumulative / baseline
