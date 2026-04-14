"""Upsert helpers for tw.intraday_quotes.

Three write paths share this module:

  * upsert_quotes — bulk writes from the REST sweeper (full OHLCV + cumulative)
  * upsert_trade  — per-tick writes from the WebSocket trades channel
  * upsert_book   — top-of-book writes from the WebSocket books channel

All three use ON CONFLICT DO UPDATE with COALESCE so a partial update (e.g. a
book tick that only knows bid/ask) never clobbers fields written by another
path. The primary key is stock_id alone, which naturally enforces the
"latest snapshot only" storage policy.
"""

from datetime import datetime

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


def upsert_quotes(records: list[dict], market: str, trade_date=None):
    """Bulk upsert from the REST sweeper.

    market: 'TSE' or 'OTC' (maps to tw.stocks.market TWSE/TPEx)
    """
    if not records:
        return 0

    tw_market = _MARKET_BY_SNAPSHOT.get(market, market)
    written = 0

    with get_cursor() as cur:
        for r in records:
            stock_id = r.get("stock_id")
            if not stock_id:
                continue
            if not _ensure_stock(cur, stock_id, r.get("name"), tw_market):
                continue

            # Note: ref_price is deliberately NOT written by the sweeper. It is
            # set once per day by the SinoPac pre-market path (upsert_reference)
            # and the sweeper's limit_up / limit_down are wrapped in COALESCE so
            # the authoritative pre-market values survive REST refreshes that
            # don't carry them.
            cur.execute(
                """
                INSERT INTO tw.intraday_quotes (
                    stock_id, trade_date,
                    open_price, high_price, low_price, last_price,
                    last_size, last_trade_at,
                    total_volume, total_value, tx_count,
                    change_price, change_pct, amplitude,
                    limit_up, limit_down,
                    source, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (stock_id) DO UPDATE SET
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
                (
                    stock_id, trade_date,
                    r.get("open_price"), r.get("high_price"), r.get("low_price"), r.get("last_price"),
                    r.get("last_size"), r.get("last_trade_at"),
                    r.get("total_volume"), r.get("total_value"), r.get("tx_count"),
                    r.get("change_price"), r.get("change_pct"), r.get("amplitude"),
                    r.get("limit_up"), r.get("limit_down"),
                    "rest_sweep",
                ),
            )
            written += 1

    return written


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
