"""REST wrapper around esun_marketdata.rest.stock.

The SDK (esun_marketdata is Fugle MarketData under E.Sun auth) returns the raw
Fugle JSON payloads. This module just:

  1. Calls the right SDK method
  2. Normalizes each row into the shape tw.intraday_quotes expects

Upstream response doc (fields may evolve):
https://developer.fugle.tw/docs/data/http-api/snapshot/quotes
"""

from datetime import datetime, timezone, timedelta
from typing import Literal


_TPE_TZ = timezone(timedelta(hours=8))


def _to_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _epoch_us_to_dt(us):
    """Fugle uses epoch microseconds for trade timestamps."""
    n = _to_int(us)
    if n is None or n <= 0:
        return None
    return datetime.fromtimestamp(n / 1_000_000, tz=_TPE_TZ)


def _normalize_snapshot(row: dict) -> dict | None:
    """Convert one snapshot element to the tw.intraday_quotes shape."""
    symbol = str(row.get("symbol", "")).strip()
    if not symbol:
        return None

    # E.Sun snapshot/quotes returns a flat row (tradeVolume, tradeValue,
    # lastUpdated at top level). Older Fugle docs show nested total/lastTrade
    # objects, so we fall back to those for resilience.
    total = row.get("total") or {}
    last_trade = row.get("lastTrade") or {}

    return {
        "stock_id":      symbol,
        "name":          (row.get("name") or "").strip(),
        "open_price":    _to_float(row.get("openPrice")),
        "high_price":    _to_float(row.get("highPrice")),
        "low_price":     _to_float(row.get("lowPrice")),
        # During the session closePrice is the latest traded price; after close
        # it is the official close. lastPrice is the fallback.
        "last_price":    _to_float(row.get("closePrice") or row.get("lastPrice")),
        "last_size":     _to_int(row.get("lastSize") or last_trade.get("size")),
        "last_trade_at": _epoch_us_to_dt(row.get("lastUpdated") or last_trade.get("at")),
        # E.Sun returns tradeVolume in lots (張). Normalize to shares (股) so
        # it matches total_value units and the daily TWSE data conventions.
        "total_volume":  (lambda v: v * 1000 if v is not None else None)(
            _to_int(row.get("tradeVolume") or total.get("tradeVolume"))
        ),
        "total_value":   _to_int(row.get("tradeValue") or total.get("tradeValue")),
        "tx_count":      _to_int(row.get("transaction") or total.get("transaction")),
        "change_price":  _to_float(row.get("change")),
        "change_pct":    _to_float(row.get("changePercent")),
        "amplitude":     _to_float(row.get("amplitude")),
        "limit_up":      _to_float(row.get("priceLimitHigh") or row.get("referencePrice")),
        "limit_down":    _to_float(row.get("priceLimitLow")),
    }


def fetch_snapshot_quotes(sdk, market: Literal["TSE", "OTC"]) -> list[dict]:
    """Fetch a full-market snapshot in a single call and normalize the rows.

    sdk: a logged-in esun_marketdata.EsunMarketdata instance
    market: 'TSE' (上市) or 'OTC' (上櫃)
    """
    # Each access to sdk.rest_client creates a fresh RestClientFactory under the
    # hood, so there's nothing to cache on our side.
    stock = sdk.rest_client.stock
    resp = stock.snapshot.quotes(market=market)

    if not isinstance(resp, dict):
        print(f"  [REST] snapshot/quotes/{market}: unexpected response type {type(resp)}")
        return []

    rows = resp.get("data") or []
    out: list[dict] = []
    for row in rows:
        n = _normalize_snapshot(row)
        if n is not None:
            out.append(n)
    return out


def fetch_intraday_candles(sdk, symbol: str, timeframe: str = "1") -> list[dict]:
    """Intraday candles for a single symbol. Used by the signal layer (Phase B)."""
    stock = sdk.rest_client.stock
    resp = stock.intraday.candles(symbol=symbol, timeframe=timeframe)
    if not isinstance(resp, dict):
        return []
    return resp.get("data") or []
