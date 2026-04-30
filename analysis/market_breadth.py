"""
Market breadth indicator — aggregate sort alignment across all non-dead-fish stocks.

For each trading day:
  1. Filter out dead_fish stocks
  2. Count sort_normal up/down for short/medium/long timeframes
  3. Output percentages: up% / down% / neutral%

Usage:
    from analysis.market_breadth import calculate_market_breadth
    result = calculate_market_breadth()
    print(result[-1])  # latest day
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

Trend = Literal[
    "bear_exhausting", "strong_bear", "bear", "neutral",
    "bull", "strong_bull", "bull_exhausting",
]

TREND_CODE: dict[Trend, int] = {
    "bear_exhausting": -3,
    "strong_bear": -2,
    "bear": -1,
    "neutral": 0,
    "bull": 1,
    "strong_bull": 2,
    "bull_exhausting": 3,
}

def classify_trend(up_pct: float, down_pct: float, neutral_pct: float) -> Trend:
    """Classify market trend from up/down/neutral percentages.

    Priority: neutral > 50% takes precedence over up/down comparison.
    """
    if neutral_pct > 50:
        return "neutral"
    if up_pct > down_pct:
        return "strong_bull" if up_pct > neutral_pct > down_pct else "bull"
    if down_pct > up_pct:
        return "strong_bear" if down_pct > neutral_pct > up_pct else "bear"
    return "neutral"


# Exhaustion: per-scope paths as (days, min_convergence_ratio).
# Spread must shrink by at least min_convergence_ratio over the period.
# Short path = fewer days but higher magnitude required.
# OR logic: any path triggering = exhausting.
SCOPE_EXHAUST_PATHS: dict[str, tuple[tuple[int, float], ...]] = {
    "short":  ((2, 0.30), (3, 0.20)),
    "medium": ((3, 0.20), (5, 0.10)),
    "long":   ((5, 0.10), (8, 0.0)),
}

# Weakening signal: today's |Δ| must exceed
# WEAKENING_THRESHOLD_RATIO × average |Δ| of past SCOPE_LOOKBACK[scope] days.
WEAKENING_THRESHOLD_RATIO = 1.0

SCOPE_LOOKBACK: dict[str, int] = {
    "short":  3,
    "medium": 3,
    "long":   3,
}


def classify_trend_series(
    up_pcts: list[float],
    down_pcts: list[float],
    scope: str = "long",
) -> list[Trend]:
    """Classify full series with spread-exhaustion overlay.

    Exhaustion: consecutive spread convergence with acceleration,
    AND spread must shrink by at least min_ratio over the period.
    OR logic across paths: short path (high magnitude) fires early,
    long path (low magnitude) catches sustained convergence.
    """
    n = len(up_pcts)
    out: list[Trend] = []
    paths = SCOPE_EXHAUST_PATHS[scope]
    spreads = [up_pcts[i] - down_pcts[i] for i in range(n)]

    for i in range(n):
        up, dn = up_pcts[i], down_pcts[i]
        base = classify_trend(up, dn, 100 - up - dn)

        bull_ex = bear_ex = False
        for days, min_ratio in paths:
            if i < days:
                continue
            deltas = [spreads[i - k] - spreads[i - k - 1] for k in range(days)]

            # Convergence magnitude: how much |spread| has shrunk
            start_spread = abs(spreads[i - days])
            if start_spread < 0.5:
                continue
            convergence = 1 - abs(spreads[i]) / start_spread

            # Bull: spread shrinking (deltas < 0) + accelerating + magnitude
            if not bull_ex:
                bull_ex = (convergence >= min_ratio
                           and all(d < 0 for d in deltas)
                           and all(abs(deltas[k]) > abs(deltas[k + 1]) for k in range(days - 1)))
            # Bear: spread shrinking (deltas > 0) + accelerating + magnitude
            if not bear_ex:
                bear_ex = (convergence >= min_ratio
                           and all(d > 0 for d in deltas)
                           and all(abs(deltas[k]) > abs(deltas[k + 1]) for k in range(days - 1)))

        if base in ("bull", "strong_bull") and bull_ex:
            out.append("bull_exhausting")
        elif base in ("bear", "strong_bear") and bear_ex:
            out.append("bear_exhausting")
        else:
            out.append(base)
    return out


def classify_weakening_series(
    up_pcts: list[float],
    down_pcts: list[float],
    trends: list[Trend],
    scope: str = "long",
) -> list[bool]:
    """Detect weakening signal: exhausting trend + today's sharp convergence.

    Requires the trend to already be exhausting (sustained spread convergence),
    AND today's own change magnitude exceeds the recent average:
      threshold = RATIO * mean(|Δ| of past LOOKBACK days)

    Bull weakening: trend is bull_exhausting AND -Δup > threshold AND Δdn > threshold.
    Bear weakening: mirrored.
    """
    n = len(up_pcts)
    out: list[bool] = []
    lb = SCOPE_LOOKBACK[scope]
    ratio = WEAKENING_THRESHOLD_RATIO

    for i in range(n):
        if i < lb + 1:
            out.append(False)
            continue

        trend = trends[i]
        up, dn = up_pcts[i], down_pcts[i]
        du = up - up_pcts[i - 1]
        dd = dn - down_pcts[i - 1]

        prev_du = [abs(up_pcts[i - k] - up_pcts[i - k - 1]) for k in range(1, lb + 1)]
        prev_dd = [abs(down_pcts[i - k] - down_pcts[i - k - 1]) for k in range(1, lb + 1)]
        th_up = ratio * (sum(prev_du) / lb)
        th_dn = ratio * (sum(prev_dd) / lb)

        is_weak = False
        if trend == "bull_exhausting" and -du > th_up and dd > th_dn:
            is_weak = True
        elif trend == "bear_exhausting" and -dd > th_dn and du > th_up:
            is_weak = True
        out.append(is_weak)
    return out

import numpy as np
from numpy.typing import NDArray

from db.connection import get_cursor
from analysis.close import calculate_close, calc_sort_forming
from analysis.money import calculate_money
from analysis.volume import calculate_volume

F32 = np.float32

# Minimum trading days required for a stock to be included
MIN_DAYS = 13


@dataclass
class BreadthDay:
    """Market breadth data for a single day."""
    trade_date: date
    active_stocks: int          # all stocks with data on this day
    total_stocks: int           # non-dead-fish stocks on this day

    short_up: int
    short_down: int
    medium_up: int
    medium_down: int
    long_up: int
    long_down: int

    # Forming sort alignment (predicted-MA based transitional state)
    short_up_forming: int
    short_down_forming: int
    medium_up_forming: int
    medium_down_forming: int
    long_up_forming: int
    long_down_forming: int

    # Total = union of normal and forming (no double-counting)
    short_up_total: int
    short_down_total: int
    medium_up_total: int
    medium_down_total: int
    long_up_total: int
    long_down_total: int

    def _pct(self, count: int) -> float:
        return count / self.total_stocks * 100 if self.total_stocks else 0

    @property
    def alive_pct(self) -> float:
        """Non-dead-fish as percentage of all active stocks."""
        return self.total_stocks / self.active_stocks * 100 if self.active_stocks else 0

    @property
    def short_up_pct(self) -> float:
        return self._pct(self.short_up)

    @property
    def short_down_pct(self) -> float:
        return self._pct(self.short_down)

    @property
    def short_neutral_pct(self) -> float:
        return self._pct(self.total_stocks - self.short_up - self.short_down)

    @property
    def medium_up_pct(self) -> float:
        return self._pct(self.medium_up)

    @property
    def medium_down_pct(self) -> float:
        return self._pct(self.medium_down)

    @property
    def medium_neutral_pct(self) -> float:
        return self._pct(self.total_stocks - self.medium_up - self.medium_down)

    @property
    def long_up_pct(self) -> float:
        return self._pct(self.long_up)

    @property
    def long_down_pct(self) -> float:
        return self._pct(self.long_down)

    @property
    def long_neutral_pct(self) -> float:
        return self._pct(self.total_stocks - self.long_up - self.long_down)

    @property
    def short_up_forming_pct(self) -> float:
        return self._pct(self.short_up_forming)

    @property
    def short_down_forming_pct(self) -> float:
        return self._pct(self.short_down_forming)

    @property
    def medium_up_forming_pct(self) -> float:
        return self._pct(self.medium_up_forming)

    @property
    def medium_down_forming_pct(self) -> float:
        return self._pct(self.medium_down_forming)

    @property
    def long_up_forming_pct(self) -> float:
        return self._pct(self.long_up_forming)

    @property
    def long_down_forming_pct(self) -> float:
        return self._pct(self.long_down_forming)

    @property
    def short_trend(self) -> Trend:
        return classify_trend(self.short_up_pct, self.short_down_pct, self.short_neutral_pct)

    @property
    def medium_trend(self) -> Trend:
        return classify_trend(self.medium_up_pct, self.medium_down_pct, self.medium_neutral_pct)

    @property
    def long_trend(self) -> Trend:
        return classify_trend(self.long_up_pct, self.long_down_pct, self.long_neutral_pct)

    @property
    def short_trend_total(self) -> Trend:
        up = self._pct(self.short_up_total)
        dn = self._pct(self.short_down_total)
        neu = self._pct(self.total_stocks - self.short_up_total - self.short_down_total)
        return classify_trend(up, dn, neu)

    @property
    def medium_trend_total(self) -> Trend:
        up = self._pct(self.medium_up_total)
        dn = self._pct(self.medium_down_total)
        neu = self._pct(self.total_stocks - self.medium_up_total - self.medium_down_total)
        return classify_trend(up, dn, neu)

    @property
    def long_trend_total(self) -> Trend:
        up = self._pct(self.long_up_total)
        dn = self._pct(self.long_down_total)
        neu = self._pct(self.total_stocks - self.long_up_total - self.long_down_total)
        return classify_trend(up, dn, neu)


def calculate_market_breadth(
    last_n_days: int = 20,
) -> list[BreadthDay]:
    """
    Calculate market breadth for the most recent N trading days.

    Iterates all active stocks, runs close + money analysis,
    then aggregates sort alignment excluding dead-fish stocks.
    """
    # 1. Get all active stock IDs
    stock_ids = _get_active_stocks()
    print(f"分析 {len(stock_ids)} 檔股票...")

    # 2. Get the common date list (last N trading days from the market)
    target_dates = _get_recent_dates(last_n_days)
    if not target_dates:
        return []

    date_set = set(target_dates)
    n_dates = len(target_dates)
    date_to_idx = {d: i for i, d in enumerate(target_dates)}

    # Per-day accumulators
    active = np.zeros(n_dates, dtype=np.int32)  # all stocks with data
    total = np.zeros(n_dates, dtype=np.int32)    # non-dead-fish
    s_up = np.zeros(n_dates, dtype=np.int32)
    s_dn = np.zeros(n_dates, dtype=np.int32)
    m_up = np.zeros(n_dates, dtype=np.int32)
    m_dn = np.zeros(n_dates, dtype=np.int32)
    l_up = np.zeros(n_dates, dtype=np.int32)
    l_dn = np.zeros(n_dates, dtype=np.int32)
    s_upf = np.zeros(n_dates, dtype=np.int32)
    s_dnf = np.zeros(n_dates, dtype=np.int32)
    m_upf = np.zeros(n_dates, dtype=np.int32)
    m_dnf = np.zeros(n_dates, dtype=np.int32)
    l_upf = np.zeros(n_dates, dtype=np.int32)
    l_dnf = np.zeros(n_dates, dtype=np.int32)
    # Total = union of normal and forming (no double-counting)
    s_upt = np.zeros(n_dates, dtype=np.int32)
    s_dnt = np.zeros(n_dates, dtype=np.int32)
    m_upt = np.zeros(n_dates, dtype=np.int32)
    m_dnt = np.zeros(n_dates, dtype=np.int32)
    l_upt = np.zeros(n_dates, dtype=np.int32)
    l_dnt = np.zeros(n_dates, dtype=np.int32)

    # 3. Process each stock
    done = 0
    for stock_id in stock_ids:
        rows = _fetch_stock_data(stock_id)
        if len(rows) < MIN_DAYS:
            continue

        close = np.array([float(r["close_price"]) for r in rows], dtype=F32)
        turnover = np.array([float(r["turnover"]) for r in rows], dtype=F32)
        volume = np.array([float(r["volume"]) for r in rows], dtype=F32)
        stock_dates = [r["trade_date"] for r in rows]

        # Run analysis
        close_result = calculate_close(close)
        money_result = calculate_money(turnover)
        volume_result = calculate_volume(volume)
        sort_forming = calc_sort_forming(close_result, volume_result.volume_status)

        sn = close_result.ma.sort_normal

        # Map stock days to target date indices
        for j in range(len(stock_dates)):
            d = stock_dates[j]
            if d not in date_set:
                continue
            idx = date_to_idx[d]
            active[idx] += 1

            # Skip dead fish
            if money_result.dead_fish[j]:
                continue

            total[idx] += 1

            # Normal counts
            snu = sn["short"].up[j];  snd = sn["short"].down[j]
            mnu = sn["medium"].up[j]; mnd = sn["medium"].down[j]
            lnu = sn["long"].up[j];   lnd = sn["long"].down[j]
            if snu: s_up[idx] += 1
            elif snd: s_dn[idx] += 1
            if mnu: m_up[idx] += 1
            elif mnd: m_dn[idx] += 1
            if lnu: l_up[idx] += 1
            elif lnd: l_dn[idx] += 1

            # Forming counts
            sfu = sort_forming["short"].up[j];  sfd = sort_forming["short"].down[j]
            mfu = sort_forming["medium"].up[j]; mfd = sort_forming["medium"].down[j]
            lfu = sort_forming["long"].up[j];   lfd = sort_forming["long"].down[j]
            if sfu: s_upf[idx] += 1
            elif sfd: s_dnf[idx] += 1
            if mfu: m_upf[idx] += 1
            elif mfd: m_dnf[idx] += 1
            if lfu: l_upf[idx] += 1
            elif lfd: l_dnf[idx] += 1

            # Total = union (normal OR forming, no double-counting)
            if snu or sfu: s_upt[idx] += 1
            elif snd or sfd: s_dnt[idx] += 1
            if mnu or mfu: m_upt[idx] += 1
            elif mnd or mfd: m_dnt[idx] += 1
            if lnu or lfu: l_upt[idx] += 1
            elif lnd or lfd: l_dnt[idx] += 1

        done += 1
        if done % 200 == 0:
            print(f"  已處理 {done}/{len(stock_ids)}...")

    print(f"  完成 {done}/{len(stock_ids)}")

    # 4. Build results
    results = []
    for i, d in enumerate(target_dates):
        results.append(BreadthDay(
            trade_date=d,
            active_stocks=int(active[i]),
            total_stocks=int(total[i]),
            short_up=int(s_up[i]),
            short_down=int(s_dn[i]),
            medium_up=int(m_up[i]),
            medium_down=int(m_dn[i]),
            long_up=int(l_up[i]),
            long_down=int(l_dn[i]),
            short_up_forming=int(s_upf[i]),
            short_down_forming=int(s_dnf[i]),
            medium_up_forming=int(m_upf[i]),
            medium_down_forming=int(m_dnf[i]),
            long_up_forming=int(l_upf[i]),
            long_down_forming=int(l_dnf[i]),
            short_up_total=int(s_upt[i]),
            short_down_total=int(s_dnt[i]),
            medium_up_total=int(m_upt[i]),
            medium_down_total=int(m_dnt[i]),
            long_up_total=int(l_upt[i]),
            long_down_total=int(l_dnt[i]),
        ))

    return results


def save_market_breadth(results: list[BreadthDay]) -> int:
    """
    Upsert market breadth results into tw.market_breadth.
    Returns the number of rows upserted.
    """
    if not results:
        return 0

    with get_cursor() as cur:
        sql = """
            INSERT INTO tw.market_breadth (
                trade_date, active_stocks, total_stocks,
                short_up, short_down, medium_up, medium_down,
                long_up, long_down,
                short_up_forming, short_down_forming,
                medium_up_forming, medium_down_forming,
                long_up_forming, long_down_forming,
                short_up_total, short_down_total,
                medium_up_total, medium_down_total,
                long_up_total, long_down_total,
                short_trend, medium_trend, long_trend,
                short_trend_total, medium_trend_total, long_trend_total
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE SET
                active_stocks          = EXCLUDED.active_stocks,
                total_stocks           = EXCLUDED.total_stocks,
                short_up               = EXCLUDED.short_up,
                short_down             = EXCLUDED.short_down,
                medium_up              = EXCLUDED.medium_up,
                medium_down            = EXCLUDED.medium_down,
                long_up                = EXCLUDED.long_up,
                long_down              = EXCLUDED.long_down,
                short_up_forming       = EXCLUDED.short_up_forming,
                short_down_forming     = EXCLUDED.short_down_forming,
                medium_up_forming      = EXCLUDED.medium_up_forming,
                medium_down_forming    = EXCLUDED.medium_down_forming,
                long_up_forming        = EXCLUDED.long_up_forming,
                long_down_forming      = EXCLUDED.long_down_forming,
                short_up_total         = EXCLUDED.short_up_total,
                short_down_total       = EXCLUDED.short_down_total,
                medium_up_total        = EXCLUDED.medium_up_total,
                medium_down_total      = EXCLUDED.medium_down_total,
                long_up_total          = EXCLUDED.long_up_total,
                long_down_total        = EXCLUDED.long_down_total,
                short_trend            = EXCLUDED.short_trend,
                medium_trend           = EXCLUDED.medium_trend,
                long_trend             = EXCLUDED.long_trend,
                short_trend_total      = EXCLUDED.short_trend_total,
                medium_trend_total     = EXCLUDED.medium_trend_total,
                long_trend_total       = EXCLUDED.long_trend_total
        """
        rows = [
            (
                r.trade_date, r.active_stocks, r.total_stocks,
                r.short_up, r.short_down, r.medium_up, r.medium_down,
                r.long_up, r.long_down,
                r.short_up_forming, r.short_down_forming,
                r.medium_up_forming, r.medium_down_forming,
                r.long_up_forming, r.long_down_forming,
                r.short_up_total, r.short_down_total,
                r.medium_up_total, r.medium_down_total,
                r.long_up_total, r.long_down_total,
                TREND_CODE[r.short_trend], TREND_CODE[r.medium_trend], TREND_CODE[r.long_trend],
                TREND_CODE[r.short_trend_total], TREND_CODE[r.medium_trend_total], TREND_CODE[r.long_trend_total],
            )
            for r in results
        ]
        cur.executemany(sql, rows)

    return len(results)


def _get_active_stocks() -> list[str]:
    """Get all active stock IDs, excluding ESB (興櫃) and ETFs."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id FROM tw.stocks "
            "WHERE is_active = TRUE AND market != 'ESB' AND security_type = 'STOCK' "
            "ORDER BY stock_id"
        )
        return [r["stock_id"] for r in cur.fetchall()]


def _get_recent_dates(n: int) -> list[date]:
    """Get the most recent N distinct trading dates."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT DISTINCT trade_date FROM tw.daily_prices
            ORDER BY trade_date DESC LIMIT %s
            """,
            (n,),
        )
        dates = [r["trade_date"] for r in cur.fetchall()]
    dates.sort()
    return dates


def _fetch_stock_data(stock_id: str) -> list[dict]:
    """Fetch close + turnover + volume for a stock (all history)."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, close_price, turnover, volume
            FROM tw.daily_prices
            WHERE stock_id = %s
              AND close_price IS NOT NULL
              AND turnover IS NOT NULL
              AND volume IS NOT NULL
            ORDER BY trade_date ASC
            """,
            (stock_id,),
        )
        return cur.fetchall()
