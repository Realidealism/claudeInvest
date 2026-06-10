"""
CNN Fear & Greed Index scraper.

Endpoint:
  https://production.dataviz.cnn.io/index/fearandgreed/graphdata

Returns a JSON blob with the headline 0-100 score plus 7 sub-indicators,
each carrying a `data: [{x: ms_timestamp, y: score, rating}]` history of
roughly the past year. We upsert every (date × score × rating) tuple into
tw.cnn_fear_greed; older rows already in DB stay intact when CNN trims
its window. The headline score for the most recent date comes from
`fear_and_greed.score` (which can drift mid-session) — historical points
use `fear_and_greed_historical.data` for stability.

This is a US-market sentiment indicator. trade_date parameter to
scrape_date() is ignored — every call refreshes the full year because
CNN's response is cheap (single GET, ~100 KB) and 1-year backfill keeps
the table self-healing if any earlier daily_update was skipped.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

CNN_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

# Headline + 7 sub-indicators. The DB column prefix is the dict key; the
# CNN payload key is the value. Multiple fallbacks per slot guard against
# CNN renaming fields (they have shipped renames historically).
SUB_INDICATORS = {
    "momentum":   ["market_momentum_sp500", "market_momentum"],
    "strength":   ["stock_price_strength"],
    "breadth":    ["stock_price_breadth"],
    "put_call":   ["put_call_options", "put_call_ratio"],
    "safe_haven": ["safe_haven_demand"],
    "junk_bond":  ["junk_bond_demand"],
    "volatility": ["market_volatility_vix", "market_volatility"],
}


def _ms_to_date(ms: int | float) -> date | None:
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date()
    except (ValueError, OSError, TypeError):
        return None


def _block_for(payload: dict, candidates: list[str]) -> dict | None:
    """Return the first matching sub-indicator block from CNN payload."""
    for key in candidates:
        block = payload.get(key)
        if isinstance(block, dict):
            return block
    return None


def _build_history_map(block: dict | None) -> dict[date, tuple[float, str | None]]:
    """Map trade_date → (score, rating) from a CNN sub-indicator block."""
    out: dict[date, tuple[float, str | None]] = {}
    if not block:
        return out
    for pt in block.get("data") or []:
        try:
            d = _ms_to_date(pt["x"])
            if d is None:
                continue
            score = float(pt["y"])
            rating = pt.get("rating")
            out[d] = (score, rating)
        except (KeyError, ValueError, TypeError):
            continue
    return out


def fetch_cnn_payload() -> dict[str, Any] | None:
    """Fetch the full graphdata JSON. CNN expects a browser-ish UA; the
    session in utils.http_client already sets Mozilla, which is enough."""
    return fetch_json_retry(
        CNN_URL,
        timeout=30,
        validate=lambda d: isinstance(d, dict) and "fear_and_greed_historical" in d,
    )


def _parse_payload(payload: dict) -> list[dict]:
    """Cross-join headline history with each sub-indicator history by date.

    Returns one dict per date with all 8 (score, rating) pairs; sub
    indicators absent for that date get None."""
    headline_hist = _build_history_map(payload.get("fear_and_greed_historical"))

    sub_maps: dict[str, dict[date, tuple[float, str | None]]] = {
        slot: _build_history_map(_block_for(payload, keys))
        for slot, keys in SUB_INDICATORS.items()
    }

    # Today's headline lives in fear_and_greed.score (more up-to-date than
    # the last historical point during a live US session).
    fg_latest = payload.get("fear_and_greed") or {}
    today_score = fg_latest.get("score")
    today_rating = fg_latest.get("rating")

    rows = []
    for d in sorted(headline_hist.keys()):
        score, rating = headline_hist[d]
        row = {
            "trade_date": d,
            "score":  round(score, 2),
            "rating": rating,
        }
        for slot in SUB_INDICATORS:
            sval = sub_maps[slot].get(d)
            if sval is not None:
                # sub_score is raw underlying value (VIX level, put/call
                # ratio, McClellan summation, etc.) — keep 4 dp of precision.
                row[f"{slot}_score"]  = round(sval[0], 4)
                row[f"{slot}_rating"] = sval[1]
            else:
                row[f"{slot}_score"]  = None
                row[f"{slot}_rating"] = None
        rows.append(row)

    # Overlay live headline onto the most recent date (CNN historical
    # closes at previous trading day during the session).
    if rows and today_score is not None:
        try:
            ts_str = fg_latest.get("timestamp")
            live_date = (
                _ms_to_date(int(fg_latest["timestamp"])) if isinstance(ts_str, (int, float))
                else date.fromisoformat(ts_str[:10]) if isinstance(ts_str, str)
                else rows[-1]["trade_date"]
            )
        except (KeyError, ValueError, TypeError):
            live_date = rows[-1]["trade_date"]
        if rows[-1]["trade_date"] == live_date:
            rows[-1]["score"] = round(float(today_score), 2)
            rows[-1]["rating"] = today_rating
        elif live_date and live_date > rows[-1]["trade_date"]:
            # Live point is ahead of historical tail — append a fresh row,
            # carrying the latest sub-indicator score from each block.
            live_row = {
                "trade_date": live_date,
                "score":  round(float(today_score), 2),
                "rating": today_rating,
            }
            for slot, keys in SUB_INDICATORS.items():
                blk = _block_for(payload, keys) or {}
                s = blk.get("score")
                live_row[f"{slot}_score"]  = round(float(s), 4) if s is not None else None
                live_row[f"{slot}_rating"] = blk.get("rating")
            rows.append(live_row)

    return rows


def save_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO tw.cnn_fear_greed (
            trade_date, score, rating,
            momentum_score,   momentum_rating,
            strength_score,   strength_rating,
            breadth_score,    breadth_rating,
            put_call_score,   put_call_rating,
            safe_haven_score, safe_haven_rating,
            junk_bond_score,  junk_bond_rating,
            volatility_score, volatility_rating
        ) VALUES (
            %(trade_date)s, %(score)s, %(rating)s,
            %(momentum_score)s,   %(momentum_rating)s,
            %(strength_score)s,   %(strength_rating)s,
            %(breadth_score)s,    %(breadth_rating)s,
            %(put_call_score)s,   %(put_call_rating)s,
            %(safe_haven_score)s, %(safe_haven_rating)s,
            %(junk_bond_score)s,  %(junk_bond_rating)s,
            %(volatility_score)s, %(volatility_rating)s
        )
        ON CONFLICT (trade_date) DO UPDATE SET
            score             = EXCLUDED.score,
            rating            = EXCLUDED.rating,
            momentum_score    = COALESCE(EXCLUDED.momentum_score,   tw.cnn_fear_greed.momentum_score),
            momentum_rating   = COALESCE(EXCLUDED.momentum_rating,  tw.cnn_fear_greed.momentum_rating),
            strength_score    = COALESCE(EXCLUDED.strength_score,   tw.cnn_fear_greed.strength_score),
            strength_rating   = COALESCE(EXCLUDED.strength_rating,  tw.cnn_fear_greed.strength_rating),
            breadth_score     = COALESCE(EXCLUDED.breadth_score,    tw.cnn_fear_greed.breadth_score),
            breadth_rating    = COALESCE(EXCLUDED.breadth_rating,   tw.cnn_fear_greed.breadth_rating),
            put_call_score    = COALESCE(EXCLUDED.put_call_score,   tw.cnn_fear_greed.put_call_score),
            put_call_rating   = COALESCE(EXCLUDED.put_call_rating,  tw.cnn_fear_greed.put_call_rating),
            safe_haven_score  = COALESCE(EXCLUDED.safe_haven_score, tw.cnn_fear_greed.safe_haven_score),
            safe_haven_rating = COALESCE(EXCLUDED.safe_haven_rating,tw.cnn_fear_greed.safe_haven_rating),
            junk_bond_score   = COALESCE(EXCLUDED.junk_bond_score,  tw.cnn_fear_greed.junk_bond_score),
            junk_bond_rating  = COALESCE(EXCLUDED.junk_bond_rating, tw.cnn_fear_greed.junk_bond_rating),
            volatility_score  = COALESCE(EXCLUDED.volatility_score, tw.cnn_fear_greed.volatility_score),
            volatility_rating = COALESCE(EXCLUDED.volatility_rating,tw.cnn_fear_greed.volatility_rating),
            fetched_at        = NOW()
    """
    with get_cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=100)
    return len(rows)


def scrape_date(trade_date: date) -> ScrapeResult:
    """trade_date is ignored — CNN endpoint always returns the full year."""
    payload = fetch_cnn_payload()
    if not payload:
        return ScrapeResult(records=0, api_rows=0, parse_errors=1)

    rows = _parse_payload(payload)
    saved = save_rows(rows)
    api_rows = len(payload.get("fear_and_greed_historical", {}).get("data") or [])
    print(f"  CNN F&G: saved {saved} rows (api={api_rows})")
    return ScrapeResult(records=saved, api_rows=api_rows, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
