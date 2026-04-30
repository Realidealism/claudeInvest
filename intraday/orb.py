"""Opening Range Breakout (ORB) detector — 30-minute variant for TWSE.

Pipeline:

  09:00        TWSE regular session opens; sweeper (intraday.sweeper) starts
               accumulating tw.intraday_quotes.high_price / low_price.
  09:30        freeze_opening_range(today) snapshots the OR for each watched
               stock into tw.intraday_opening_range.
  09:30-13:30  tick(today) is called on the sweep cadence (20s) and writes
               breakout / PT1 / PT2 / reversal events into
               tw.intraday_orb_signals.

Watch universe (build_orb_watchlist) is the UNION of:
  1. User watchlist  (portfolio.watchlist WHERE market='TW')
  2. Fund / ETF holdings (latest tw.fund_holdings_monthly + tw.etf_holdings)
  3. Optional: yesterday's non-dead-fish stocks from tw.stock_liquidity_daily

Always excluded: today's halted list (tw.stock_halts_today).

Profit targets use ChrisMoody's classic multipliers:
  PT1 = breakout ± 0.5 × or_range
  PT2 = breakout ± 1.0 × or_range
"""

from __future__ import annotations

from datetime import date, datetime, time as dtime, timedelta, timezone

from db.connection import get_cursor


_TPE_TZ = timezone(timedelta(hours=8))

_SESSION_OPEN       = dtime(hour=9,  minute=0)
_OR_END             = dtime(hour=9,  minute=30)   # 30-minute opening range
_SESSION_CLOSE      = dtime(hour=13, minute=30)

_PT1_MULT = 0.5
_PT2_MULT = 1.0


# ---------------------------------------------------------------------------
# Watch universe
# ---------------------------------------------------------------------------

def build_orb_watchlist(trade_date: date, include_liquid: bool = False) -> list[str]:
    """Return the deduped stock_id list to monitor for trade_date.

    include_liquid=True additionally pulls non-dead-fish stocks from the
    previous trading day's tw.stock_liquidity_daily row. This is the
    "有餘力才做" expansion from ~130 to ~1300 symbols.

    Today's halted stocks (tw.stock_halts_today) are always excluded, as are
    delisted / non-equity instruments.
    """
    tickers: set[str] = set()

    with get_cursor(commit=False) as cur:
        # 1. User watchlist
        cur.execute(
            "SELECT symbol FROM portfolio.watchlist WHERE market = 'TW'"
        )
        tickers.update(r["symbol"].strip() for r in cur.fetchall())

        # 2a. Fund top-10 holdings (latest period)
        cur.execute(
            """
            SELECT DISTINCT ticker FROM tw.fund_holdings_monthly
            WHERE period = (SELECT MAX(period) FROM tw.fund_holdings_monthly)
            """
        )
        tickers.update(r["ticker"].strip() for r in cur.fetchall())

        # 2b. ETF holdings (latest trade_date per ETF)
        cur.execute(
            """
            SELECT DISTINCT stock_id FROM tw.etf_holdings
            WHERE trade_date = (SELECT MAX(trade_date) FROM tw.etf_holdings)
            """
        )
        tickers.update(r["stock_id"].strip() for r in cur.fetchall())

        # 3. Optional: non-dead-fish names from yesterday
        if include_liquid:
            cur.execute(
                """
                SELECT stock_id FROM tw.stock_liquidity_daily
                WHERE trade_date = (
                        SELECT MAX(trade_date)
                        FROM tw.stock_liquidity_daily
                        WHERE trade_date < %s
                      )
                  AND is_dead_fish = FALSE
                  AND is_halted    = FALSE
                """,
                (trade_date,),
            )
            tickers.update(r["stock_id"].strip() for r in cur.fetchall())

        # Halt filter (always applied)
        cur.execute(
            "SELECT stock_id FROM tw.stock_halts_today WHERE trade_date = %s",
            (trade_date,),
        )
        halted = {r["stock_id"] for r in cur.fetchall()}

        # Only keep symbols that match classify_tw_security, and are active.
        # Doing the filter in SQL keeps this cheap even at ~2000 tickers.
        if not tickers:
            return []
        cur.execute(
            """
            SELECT stock_id FROM tw.stocks
            WHERE stock_id = ANY(%s)
              AND is_active = TRUE
              AND security_type IN ('STOCK', 'EQUITY_ETF', 'BOND_ETF')
            """,
            (list(tickers),),
        )
        eligible = {r["stock_id"] for r in cur.fetchall()}

    return sorted(eligible - halted)


# ---------------------------------------------------------------------------
# Freeze at 09:30
# ---------------------------------------------------------------------------

def freeze_opening_range(trade_date: date, watchlist: list[str] | None = None) -> int:
    """Snapshot the 09:00–09:30 high/low for every watched stock.

    Reads tw.intraday_quotes which by 09:30 holds the highest/lowest trade
    seen since the session open. Idempotent within the day.

    Returns number of OR rows written. Stocks without usable OR data
    (missing high/low, or high==low due to no trades) are skipped silently.
    """
    if watchlist is None:
        watchlist = build_orb_watchlist(trade_date)
    if not watchlist:
        return 0

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT stock_id, high_price, low_price
            FROM tw.intraday_quotes
            WHERE stock_id = ANY(%s)
              AND trade_date = %s
              AND high_price IS NOT NULL
              AND low_price  IS NOT NULL
              AND high_price > 0
              AND low_price  > 0
            """,
            (watchlist, trade_date),
        )
        rows = cur.fetchall()

        written = 0
        for r in rows:
            hi = float(r["high_price"])
            lo = float(r["low_price"])
            if hi <= lo:
                # no real range established yet (e.g., single print or halted)
                continue
            cur.execute(
                """
                INSERT INTO tw.intraday_opening_range
                    (trade_date, stock_id, or_high, or_low, or_range, established_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (trade_date, stock_id) DO UPDATE SET
                    or_high        = EXCLUDED.or_high,
                    or_low         = EXCLUDED.or_low,
                    or_range       = EXCLUDED.or_range,
                    established_at = NOW()
                """,
                (trade_date, r["stock_id"], hi, lo, hi - lo),
            )
            written += 1

    return written


# ---------------------------------------------------------------------------
# Per-tick breakout / PT / reversal detection
# ---------------------------------------------------------------------------

def tick(trade_date: date, now: datetime | None = None) -> dict:
    """Evaluate all watched stocks against their OR and update orb_signals.

    Single pass over tw.intraday_quotes JOIN tw.intraday_opening_range.
    For each stock:
      - If no signal row yet: record first breakout (U or D) when last_price
        crosses or_high / or_low.
      - If signal exists and pt1_hit_at is NULL: check PT1 level.
      - If pt2_hit_at is NULL: check PT2 level.
      - If reversed_at is NULL: check crossing back through the OPPOSITE OR
        side (fakeout signal).

    Returns a counters dict: {'breakouts', 'pt1', 'pt2', 'reversals'}.
    """
    now = now or datetime.now(_TPE_TZ)
    counters = {"breakouts": 0, "pt1": 0, "pt2": 0, "reversals": 0}

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                r.stock_id,
                r.or_high,
                r.or_low,
                r.or_range,
                q.last_price,
                s.direction,
                s.pt1_hit_at,
                s.pt2_hit_at,
                s.reversed_at
            FROM tw.intraday_opening_range r
            JOIN tw.intraday_quotes q
                ON q.stock_id = r.stock_id AND q.trade_date = r.trade_date
            LEFT JOIN tw.intraday_orb_signals s
                ON s.stock_id = r.stock_id AND s.trade_date = r.trade_date
            WHERE r.trade_date = %s
              AND q.last_price IS NOT NULL
              AND q.last_price > 0
            """,
            (trade_date,),
        )
        rows = cur.fetchall()

        for r in rows:
            stock_id = r["stock_id"]
            price = float(r["last_price"])
            hi = float(r["or_high"])
            lo = float(r["or_low"])
            rng = float(r["or_range"])
            direction = r["direction"]

            # Stage 1: first breakout
            if direction is None:
                if price > hi:
                    _insert_breakout(cur, trade_date, stock_id, "U", price, now)
                    counters["breakouts"] += 1
                elif price < lo:
                    _insert_breakout(cur, trade_date, stock_id, "D", price, now)
                    counters["breakouts"] += 1
                continue

            # Stages 2+: targets and reversal
            if direction == "U":
                pt1_level = hi + _PT1_MULT * rng
                pt2_level = hi + _PT2_MULT * rng
                reverse_level = lo  # price back through OR low
                if r["pt1_hit_at"] is None and price >= pt1_level:
                    _mark_stage(cur, trade_date, stock_id, "pt1_hit_at", now)
                    counters["pt1"] += 1
                if r["pt2_hit_at"] is None and price >= pt2_level:
                    _mark_stage(cur, trade_date, stock_id, "pt2_hit_at", now)
                    counters["pt2"] += 1
                if r["reversed_at"] is None and price <= reverse_level:
                    _mark_stage(cur, trade_date, stock_id, "reversed_at", now)
                    counters["reversals"] += 1
            else:  # direction == 'D'
                pt1_level = lo - _PT1_MULT * rng
                pt2_level = lo - _PT2_MULT * rng
                reverse_level = hi
                if r["pt1_hit_at"] is None and price <= pt1_level:
                    _mark_stage(cur, trade_date, stock_id, "pt1_hit_at", now)
                    counters["pt1"] += 1
                if r["pt2_hit_at"] is None and price <= pt2_level:
                    _mark_stage(cur, trade_date, stock_id, "pt2_hit_at", now)
                    counters["pt2"] += 1
                if r["reversed_at"] is None and price >= reverse_level:
                    _mark_stage(cur, trade_date, stock_id, "reversed_at", now)
                    counters["reversals"] += 1

    return counters


def _insert_breakout(cur, trade_date, stock_id: str, direction: str,
                     price: float, now: datetime) -> None:
    cur.execute(
        """
        INSERT INTO tw.intraday_orb_signals
            (trade_date, stock_id, direction, breakout_price, breakout_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (trade_date, stock_id) DO NOTHING
        """,
        (trade_date, stock_id, direction, price, now),
    )


def _mark_stage(cur, trade_date, stock_id: str, column: str, now: datetime) -> None:
    # Whitelist column names — we only ever call this with known-safe values.
    assert column in {"pt1_hit_at", "pt2_hit_at", "reversed_at"}
    cur.execute(
        f"""
        UPDATE tw.intraday_orb_signals
        SET {column} = %s, updated_at = NOW()
        WHERE trade_date = %s AND stock_id = %s AND {column} IS NULL
        """,
        (now, trade_date, stock_id),
    )


# ---------------------------------------------------------------------------
# Thread runner
# ---------------------------------------------------------------------------

def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _seconds_until(target_time: dtime, now: datetime) -> float:
    target = now.replace(
        hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0
    )
    delta = (target - now).total_seconds()
    return max(0.0, delta)


def run(stop_event, interval_sec: int = 20, include_liquid: bool = False) -> None:
    """Thread entry: wait for 09:30, freeze OR, then tick until 13:30.

    Across days:
      weekend / non-session      sleep in 5-minute chunks (responsive shutdown)
      09:30 same day             freeze OR
      13:30 next session close   stop ticking, wait for tomorrow 09:30
    """
    print(f"[ORB] starting, interval={interval_sec}s, include_liquid={include_liquid}")
    frozen_for: date | None = None

    while not stop_event.is_set():
        now = _now_tpe()
        today = now.date()

        # Weekends: coarse sleep.
        if today.weekday() >= 5:
            if stop_event.wait(300.0):
                break
            continue

        # Before 09:30: nothing to do yet.
        if now.time() < _OR_END:
            sleep_for = min(_seconds_until(_OR_END, now), 60.0)
            if stop_event.wait(sleep_for):
                break
            continue

        # Past session close: wait until tomorrow.
        if now.time() >= _SESSION_CLOSE:
            if stop_event.wait(300.0):
                break
            continue

        # 09:30 boundary → freeze OR (once per day).
        if frozen_for != today:
            try:
                n = freeze_opening_range(today)
                print(f"[ORB] {today} OR frozen for {n} stocks")
                frozen_for = today
            except Exception as e:
                print(f"[ORB] [ERROR] freeze failed: {e}")
                # Try again next tick rather than wedging the thread.
                if stop_event.wait(interval_sec):
                    break
                continue

        # 09:30 → 13:30: evaluate OR signals.
        try:
            counters = tick(today, now)
            if any(counters.values()):
                print(
                    f"[ORB] {now:%H:%M:%S} "
                    f"BO={counters['breakouts']} PT1={counters['pt1']} "
                    f"PT2={counters['pt2']} REV={counters['reversals']}"
                )
        except Exception as e:
            print(f"[ORB] [ERROR] tick failed: {e}")

        if stop_event.wait(interval_sec):
            break

    print("[ORB] stopping")
