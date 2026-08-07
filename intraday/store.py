"""Upsert helpers for tw.intraday_quotes and tw.intraday_value_profile.

Four write paths share this module:

  * upsert_quotes — bulk writes from the REST sweeper (full OHLCV + cumulative)
  * upsert_trade  — per-tick writes from the WebSocket trades channel
  * upsert_book   — top-of-book writes from the WebSocket books channel

All three use ON CONFLICT DO UPDATE with COALESCE so a partial update (e.g. a
book tick that only knows bid/ask) never clobbers fields written by another
path. The primary key is stock_id alone, which naturally enforces the
"latest snapshot only" storage policy.
"""

from datetime import datetime

from psycopg2.extras import execute_values

from db.connection import get_cursor
from utils.classifier import classify_tw_security


_MARKET_BY_SNAPSHOT = {"TSE": "TWSE", "OTC": "TPEx"}


def _ensure_stock(cur, stock_id: str, name: str | None, market: str):
    """Best-effort upsert into tw.stocks so the FK on intraday_quotes is satisfied.

    Mirrors the pattern in scrapers/institutional.py:_upsert_stocks. Rows that
    can't be classified are skipped silently — they'd fail the FK anyway.
    """
    security_type = classify_tw_security(stock_id)
    if not security_type:
        return False
    cur.execute(
        """
        INSERT INTO tw.stocks (stock_id, name, market, security_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (stock_id) DO NOTHING
        """,
        (stock_id, name or stock_id, market, security_type),
    )
    return True


def _prepare_quote_rows(records: list[dict], tw_market: str, trade_date):
    """Turn a sweep snapshot into (stock_rows, quote_rows) ready for insertion.

    Split out from the DB call so the filtering and de-duplication are
    testable without a database.

    De-duplication is not optional: the rows go in through execute_values, and
    Postgres rejects a multi-row INSERT whose ON CONFLICT DO UPDATE would touch
    the same row twice ("cannot affect row a second time"). The per-row loop
    this replaced never hit that because each statement stood alone. Later
    records win, matching the old behaviour where the last write survived.
    """
    stock_rows: dict[str, tuple] = {}
    quote_rows: dict[str, tuple] = {}

    for r in records:
        stock_id = r.get("stock_id")
        if not stock_id:
            continue
        # Unclassifiable ids would fail the FK into tw.stocks anyway.
        security_type = classify_tw_security(stock_id)
        if not security_type:
            continue

        stock_rows[stock_id] = (
            stock_id, r.get("name") or stock_id, tw_market, security_type,
        )
        # Note: ref_price is deliberately NOT written by the sweeper. It is
        # set once per day by the SinoPac pre-market path (upsert_reference)
        # and the sweeper's limit_up / limit_down are wrapped in COALESCE so
        # the authoritative pre-market values survive REST refreshes that
        # don't carry them.
        quote_rows[stock_id] = (
            stock_id, trade_date,
            r.get("open_price"), r.get("high_price"), r.get("low_price"), r.get("last_price"),
            r.get("last_size"), r.get("last_trade_at"),
            r.get("total_volume"), r.get("total_value"), r.get("tx_count"),
            r.get("change_price"), r.get("change_pct"), r.get("amplitude"),
            r.get("limit_up"), r.get("limit_down"),
            "rest_sweep",
        )

    return list(stock_rows.values()), list(quote_rows.values())


_STOCKS_INSERT = """
    INSERT INTO tw.stocks (stock_id, name, market, security_type)
    VALUES %s
    ON CONFLICT (stock_id) DO NOTHING
"""

_QUOTES_UPSERT = """
                INSERT INTO tw.intraday_quotes (
                    stock_id, trade_date,
                    open_price, high_price, low_price, last_price,
                    last_size, last_trade_at,
                    total_volume, total_value, tx_count,
                    change_price, change_pct, amplitude,
                    limit_up, limit_down,
                    source, updated_at
                )
                VALUES %s
                ON CONFLICT (stock_id) DO UPDATE SET"""


def upsert_quotes(records: list[dict], market: str, trade_date=None):
    """Bulk upsert from the REST sweeper.

    market: 'TSE' or 'OTC' (maps to tw.stocks.market TWSE/TPEx)

    A full sweep carries ~2200 rows. Sending them one statement at a time cost
    two round trips each -- about 2.8s of the ~3.5s each cycle spent working,
    which is why the 20s interval was observed landing at 23-24s.
    """
    if not records:
        return 0

    tw_market = _MARKET_BY_SNAPSHOT.get(market, market)
    stock_rows, quote_rows = _prepare_quote_rows(records, tw_market, trade_date)
    if not quote_rows:
        return 0

    with get_cursor() as cur:
        execute_values(cur, _STOCKS_INSERT, stock_rows, page_size=1000)
        execute_values(
            cur,
            _QUOTES_UPSERT + """
                    trade_date    = COALESCE(EXCLUDED.trade_date,    tw.intraday_quotes.trade_date),
                    open_price    = COALESCE(EXCLUDED.open_price,    tw.intraday_quotes.open_price),
                    high_price    = COALESCE(EXCLUDED.high_price,    tw.intraday_quotes.high_price),
                    low_price     = COALESCE(EXCLUDED.low_price,     tw.intraday_quotes.low_price),
                    last_price    = COALESCE(EXCLUDED.last_price,    tw.intraday_quotes.last_price),
                    last_size     = COALESCE(EXCLUDED.last_size,     tw.intraday_quotes.last_size),
                    last_trade_at = COALESCE(EXCLUDED.last_trade_at, tw.intraday_quotes.last_trade_at),
                    total_volume  = COALESCE(EXCLUDED.total_volume,  tw.intraday_quotes.total_volume),
                    total_value   = COALESCE(EXCLUDED.total_value,   tw.intraday_quotes.total_value),
                    tx_count      = COALESCE(EXCLUDED.tx_count,      tw.intraday_quotes.tx_count),
                    change_price  = COALESCE(EXCLUDED.change_price,  tw.intraday_quotes.change_price),
                    change_pct    = COALESCE(EXCLUDED.change_pct,    tw.intraday_quotes.change_pct),
                    amplitude     = COALESCE(EXCLUDED.amplitude,     tw.intraday_quotes.amplitude),
                    limit_up      = COALESCE(EXCLUDED.limit_up,      tw.intraday_quotes.limit_up),
                    limit_down    = COALESCE(EXCLUDED.limit_down,    tw.intraday_quotes.limit_down),
                    source        = EXCLUDED.source,
                    updated_at    = NOW()
            """,
            quote_rows,
            # 17 placeholders plus the server-side NOW(); keep in step with the
            # column list in _QUOTES_UPSERT and the tuple in _prepare_quote_rows.
            template="(" + ", ".join(["%s"] * 17) + ", NOW())",
            page_size=500,
        )

    return len(quote_rows)


def upsert_trade(stock_id: str, last_price: float, last_size: int | None,
                 last_trade_at: datetime | None,
                 total_volume: int | None = None,
                 total_value: int | None = None):
    """Write a single trade tick from the WebSocket trades channel.

    Only mutates columns the WS trade message actually carries; everything else
    is preserved via COALESCE (OHLC / amplitude / limits stay pinned from the
    most recent REST sweep).
    """
    with get_cursor() as cur:
        # Row must exist for the WS path because the sweeper is responsible for
        # creating it + the underlying tw.stocks row. If the sweeper hasn't run
        # yet, skip gracefully.
        cur.execute(
            """
            UPDATE tw.intraday_quotes SET
                last_price    = %s,
                last_size     = COALESCE(%s, last_size),
                last_trade_at = COALESCE(%s, last_trade_at),
                total_volume  = COALESCE(%s, total_volume),
                total_value   = COALESCE(%s, total_value),
                source        = 'ws_trade',
                updated_at    = NOW()
            WHERE stock_id = %s
            """,
            (last_price, last_size, last_trade_at, total_volume, total_value, stock_id),
        )


def upsert_book(stock_id: str,
                bid_price: float | None, bid_size: int | None,
                ask_price: float | None, ask_size: int | None):
    """Write a top-of-book update from the WebSocket books channel."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE tw.intraday_quotes SET
                bid_price = COALESCE(%s, bid_price),
                bid_size  = COALESCE(%s, bid_size),
                ask_price = COALESCE(%s, ask_price),
                ask_size  = COALESCE(%s, ask_size),
                source    = 'ws_book',
                updated_at = NOW()
            WHERE stock_id = %s
            """,
            (bid_price, bid_size, ask_price, ask_size, stock_id),
        )


def upsert_reference(records: list[dict], trade_date) -> int:
    """
    Pre-market write path: SinoPac Shioaji Contracts → intraday_quotes.

    Records carry (stock_id, name, market, ref_price, limit_up, limit_down).
    This path is the authoritative source for ref_price / limit_up / limit_down
    so those three columns are overwritten directly (no COALESCE). Everything
    else on the row is preserved.

    The FK into tw.stocks is satisfied via _ensure_stock, exactly like the
    REST sweeper path.
    """
    if not records:
        return 0

    written = 0
    with get_cursor() as cur:
        for r in records:
            stock_id = r.get("stock_id")
            if not stock_id:
                continue
            if not _ensure_stock(cur, stock_id, r.get("name"), r.get("market", "TWSE")):
                continue

            cur.execute(
                """
                INSERT INTO tw.intraday_quotes (
                    stock_id, trade_date,
                    ref_price, limit_up, limit_down,
                    category, day_trade, margin_balance, short_balance,
                    source, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (stock_id) DO UPDATE SET
                    trade_date     = EXCLUDED.trade_date,
                    ref_price      = EXCLUDED.ref_price,
                    limit_up       = EXCLUDED.limit_up,
                    limit_down     = EXCLUDED.limit_down,
                    category       = EXCLUDED.category,
                    day_trade      = EXCLUDED.day_trade,
                    margin_balance = EXCLUDED.margin_balance,
                    short_balance  = EXCLUDED.short_balance,
                    source         = 'sinopac_pre',
                    updated_at     = NOW()
                """,
                (
                    stock_id, trade_date,
                    r.get("ref_price"), r.get("limit_up"), r.get("limit_down"),
                    r.get("category"), r.get("day_trade"), r.get("margin_balance"), r.get("short_balance"),
                    "sinopac_pre",
                ),
            )
            written += 1

    return written


def upsert_value_profile(trade_date, time_bucket: str, market_total_value: int):
    """Write one market-wide cumulative value data point for the h(t) curve.

    Called by the sweeper after each TSE+OTC sweep. ON CONFLICT keeps the
    greater value since cumulative turnover only increases within a session.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO tw.intraday_value_profile
                (trade_date, time_bucket, market_total_value, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (trade_date, time_bucket) DO UPDATE SET
                market_total_value = GREATEST(
                    tw.intraday_value_profile.market_total_value,
                    EXCLUDED.market_total_value
                ),
                updated_at = NOW()
            """,
            (trade_date, time_bucket, market_total_value),
        )


def upsert_stock_halts(codes: list[str], trade_date) -> int:
    """Replace today's halted-stock set in tw.stock_halts_today.

    Idempotent: deletes existing rows for trade_date first so rerunning
    pre_market_update doesn't accumulate stale entries if a stock came
    off halt between runs. Rows whose stock_id isn't in tw.stocks are
    skipped (FK would otherwise fail).
    """
    if not codes:
        # Still clear any prior rows for idempotency.
        with get_cursor() as cur:
            cur.execute(
                "DELETE FROM tw.stock_halts_today WHERE trade_date = %s",
                (trade_date,),
            )
        return 0

    with get_cursor() as cur:
        # Filter to codes that actually exist in tw.stocks and are active —
        # delisted codes also carry reference=0 on the Shioaji side.
        cur.execute(
            """
            SELECT stock_id FROM tw.stocks
            WHERE stock_id = ANY(%s) AND is_active = TRUE
            """,
            (codes,),
        )
        live = [r[0] if isinstance(r, tuple) else r["stock_id"] for r in cur.fetchall()]

        cur.execute(
            "DELETE FROM tw.stock_halts_today WHERE trade_date = %s",
            (trade_date,),
        )
        written = 0
        for code in live:
            cur.execute(
                """
                INSERT INTO tw.stock_halts_today (trade_date, stock_id, detected_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (trade_date, stock_id) DO NOTHING
                """,
                (trade_date, code),
            )
            written += 1
        return written


def has_close_bucket(trade_date) -> bool:
    """Return True when the 13:30 bucket is already written for trade_date.

    Retained for h(t) curve-completeness checks (the EMA needs every
    bucket from 09:05 to 13:30). The snapshot daemon's "is sweep done
    post-close?" gate uses post_close_bucket_count instead — bucket
    '13:30' lands one sweep cycle BEFORE the closing-auction match, so
    waiting on it alone misses the auction.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT 1
            FROM tw.intraday_value_profile
            WHERE trade_date = %s AND time_bucket = '13:30'
            LIMIT 1
            """,
            (trade_date,),
        )
        return cur.fetchone() is not None


def post_close_bucket_count(trade_date) -> int:
    """Return how many > '13:30' buckets exist for trade_date.

    The snapshot daemon delays its final pass until this count crosses
    a threshold so each post-close sweep gives E.Sun more time to
    propagate the closing-auction match for stocks that settle slowly
    (thin/halted/process-stock special cases).
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM tw.intraday_value_profile
            WHERE trade_date = %s AND time_bucket > '13:30'
            """,
            (trade_date,),
        )
        row = cur.fetchone()
    if row is None:
        return 0
    n = row["n"] if isinstance(row, dict) else row[0]
    return int(n or 0)


def check_day_completeness(trade_date) -> tuple[bool, list[str]]:
    """Verify trade_date has every expected 5-min bucket from 09:05 to 13:30.

    Returns (is_complete, missing_buckets). Extra buckets (e.g. 13:35 from a
    post-close sweep) are intentionally ignored — they don't break the h(t)
    curve since _all_buckets() bounds the range used by the EMA.
    """
    from intraday.estimate import _all_buckets
    expected = set(_all_buckets())
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT time_bucket
            FROM tw.intraday_value_profile
            WHERE trade_date = %s
            """,
            (trade_date,),
        )
        have = {r["time_bucket"] if isinstance(r, dict) else r[0] for r in cur.fetchall()}
    missing = sorted(expected - have)
    return (not missing, missing)


def purge_day_profile(trade_date) -> int:
    """Delete every tw.intraday_value_profile row for trade_date.

    Called when end-of-day integrity check finds missing buckets — partial
    days poison the h(t) EMA more than dropping them does. Returns deleted
    row count.
    """
    with get_cursor() as cur:
        cur.execute(
            "DELETE FROM tw.intraday_value_profile WHERE trade_date = %s",
            (trade_date,),
        )
        return cur.rowcount
