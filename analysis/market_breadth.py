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
            if sn["short"].up[j]:
                s_up[idx] += 1
            elif sn["short"].down[j]:
                s_dn[idx] += 1

            if sn["medium"].up[j]:
                m_up[idx] += 1
            elif sn["medium"].down[j]:
                m_dn[idx] += 1

            if sn["long"].up[j]:
                l_up[idx] += 1
            elif sn["long"].down[j]:
                l_dn[idx] += 1

            if sort_forming["short"].up[j]:
                s_upf[idx] += 1
            elif sort_forming["short"].down[j]:
                s_dnf[idx] += 1

            if sort_forming["medium"].up[j]:
                m_upf[idx] += 1
            elif sort_forming["medium"].down[j]:
                m_dnf[idx] += 1

            if sort_forming["long"].up[j]:
                l_upf[idx] += 1
            elif sort_forming["long"].down[j]:
                l_dnf[idx] += 1

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
                long_up_forming, long_down_forming
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE SET
                active_stocks       = EXCLUDED.active_stocks,
                total_stocks        = EXCLUDED.total_stocks,
                short_up            = EXCLUDED.short_up,
                short_down          = EXCLUDED.short_down,
                medium_up           = EXCLUDED.medium_up,
                medium_down         = EXCLUDED.medium_down,
                long_up             = EXCLUDED.long_up,
                long_down           = EXCLUDED.long_down,
                short_up_forming    = EXCLUDED.short_up_forming,
                short_down_forming  = EXCLUDED.short_down_forming,
                medium_up_forming   = EXCLUDED.medium_up_forming,
                medium_down_forming = EXCLUDED.medium_down_forming,
                long_up_forming     = EXCLUDED.long_up_forming,
                long_down_forming   = EXCLUDED.long_down_forming
        """
        rows = [
            (
                r.trade_date, r.active_stocks, r.total_stocks,
                r.short_up, r.short_down, r.medium_up, r.medium_down,
                r.long_up, r.long_down,
                r.short_up_forming, r.short_down_forming,
                r.medium_up_forming, r.medium_down_forming,
                r.long_up_forming, r.long_down_forming,
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
