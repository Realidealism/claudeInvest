"""
富台指數 → 理論 TAIEX scraper.

Source:
  鉅亨網 (cnyes) public quote API
  https://ws.api.cnyes.com/ws/api/v1/quote/quotes/GF:TWNCON:FUTURES

`TWNCON` is the continuous front-month SGX FTSE Taiwan Index Futures (富台
指數; the contract was the MSCI Taiwan / 摩台 future before SGX switched the
index licence to FTSE in 2020). cnyes is used over Yahoo because its quote
**includes the night session** (15:00–05:00 TPE, tracking US hours) — which
is exactly when a TW holiday's biggest moves happen — whereas Yahoo's free
feed only carries the Asian day session. The continuous symbol also rolls
the front month automatically, so no month-code juggling is needed.

We convert the future's % move since the last TW cash close onto TAIEX:

    theoretical_taiex = taiex_ref_close * (ftse_now / ftse_base)

  ftse_now  : current 富台 price (cnyes field "6"), incl. night session
  ftse_base : 富台 close on the last TW cash trading day
  taiex_ref : latest TAIEX close from tw.index_prices

ftse_base is anchored to the last TW close and CARRIED FORWARD across a
holiday/weekend stretch: on the first run after a TW close, base = the
future's previous settlement (field "6" − field "11", which equals that
day's close); on later days of the same closed stretch (same taiex_ref_date)
we reuse the stored base so a multi-day holiday stays cumulative-since-TW-
close rather than resetting to each night session's daily settlement.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from db.connection import get_cursor
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

QUOTE_URL = "https://ws.api.cnyes.com/ws/api/v1/quote/quotes/{symbol}"
SYM_FTSE = "GF:TWNCON:FUTURES"  # SGX FTSE Taiwan continuous front month
SYM_TXF = "TWF:TXF:FUTURES"     # TAIFEX 台指期大台 continuous front month
FRONT_CONTRACT = "TWNCON"

# cnyes quote field codes
F_LAST = "6"      # last price
F_CHG = "11"      # change vs previous settlement (points)
F_CHGPCT = "56"   # change %
F_TIME = "200007" # last quote epoch (seconds)


def _fetch_quote(symbol: str) -> dict | None:
    data = fetch_json_retry(
        QUOTE_URL.format(symbol=symbol),
        timeout=30,
        validate=lambda d: (
            isinstance(d, dict)
            and isinstance(d.get("data"), list)
            and bool(d["data"])
            and isinstance(d["data"][0], dict)
            and d["data"][0].get(F_LAST) is not None
        ),
    )
    if not data:
        return None
    return data["data"][0]


def _epoch_to_utc(v) -> datetime | None:
    return datetime.fromtimestamp(int(v), tz=timezone.utc) if isinstance(v, (int, float)) else None


def _fetch_txf() -> dict:
    """台指期大台夜盤 (TXF) snapshot. Already in TAIEX points → its price IS
    the implied open, no conversion. Best-effort: a failure leaves the TXF
    columns null without affecting the 富台 main flow."""
    q = _fetch_quote(SYM_TXF)
    if q is None:
        print("  FTSE-TW: cnyes returned no TXF quote (night-session comparison skipped).")
        return {"txf_now": None, "txf_pct_change": None, "txf_captured_at": None}
    try:
        now = float(q[F_LAST])
        pct = float(q[F_CHGPCT]) / 100.0 if q.get(F_CHGPCT) is not None else None
    except (TypeError, ValueError):
        return {"txf_now": None, "txf_pct_change": None, "txf_captured_at": None}
    return {
        "txf_now":         round(now, 2),
        "txf_pct_change":  round(pct, 6) if pct is not None else None,
        "txf_captured_at": _epoch_to_utc(q.get(F_TIME)),
    }


def _latest_taiex() -> tuple[date, float] | None:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, close_price
            FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND close_price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return row["trade_date"], float(row["close_price"])


def _carried_base(taiex_ref_date: date) -> float | None:
    """The 富台 base already stored for this TW-close period, if any.

    Reusing it keeps a multi-day holiday cumulative-since-TW-close instead of
    drifting to each night session's daily settlement."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT ftse_base
            FROM tw.ftse_taiwan
            WHERE taiex_ref_date = %s AND ftse_base IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (taiex_ref_date,),
        )
        row = cur.fetchone()
    return float(row["ftse_base"]) if row else None


def _save(record: dict[str, Any]) -> None:
    sql = """
        INSERT INTO tw.ftse_taiwan
            (trade_date, front_contract, ftse_now, ftse_base, pct_change,
             taiex_ref_date, taiex_ref_close, theoretical_taiex, captured_at,
             txf_now, txf_pct_change, txf_captured_at)
        VALUES
            (%(trade_date)s, %(front_contract)s, %(ftse_now)s, %(ftse_base)s,
             %(pct_change)s, %(taiex_ref_date)s, %(taiex_ref_close)s,
             %(theoretical_taiex)s, %(captured_at)s,
             %(txf_now)s, %(txf_pct_change)s, %(txf_captured_at)s)
        ON CONFLICT (trade_date) DO UPDATE SET
            front_contract    = EXCLUDED.front_contract,
            ftse_now          = EXCLUDED.ftse_now,
            ftse_base         = EXCLUDED.ftse_base,
            pct_change        = EXCLUDED.pct_change,
            taiex_ref_date    = EXCLUDED.taiex_ref_date,
            taiex_ref_close   = EXCLUDED.taiex_ref_close,
            theoretical_taiex = EXCLUDED.theoretical_taiex,
            captured_at       = EXCLUDED.captured_at,
            txf_now           = EXCLUDED.txf_now,
            txf_pct_change    = EXCLUDED.txf_pct_change,
            txf_captured_at   = EXCLUDED.txf_captured_at,
            fetched_at        = NOW()
    """
    with get_cursor() as cur:
        cur.execute(sql, record)


def scrape_date(trade_date: date) -> ScrapeResult:
    """Fetch continuous 富台 from cnyes, convert to theoretical TAIEX, upsert.

    trade_date is the calendar day this snapshot belongs to (the row key);
    the conversion always uses the latest available 富台 price and the
    latest TAIEX close, regardless of trade_date."""
    taiex = _latest_taiex()
    if taiex is None:
        print("  FTSE-TW: no TAIEX reference in tw.index_prices — skipping.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=1)
    taiex_ref_date, taiex_ref_close = taiex

    q = _fetch_quote(SYM_FTSE)
    if q is None:
        print("  FTSE-TW: cnyes returned no 富台 quote.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=1)

    try:
        ftse_now = float(q[F_LAST])
        chg = float(q.get(F_CHG) or 0.0)
    except (TypeError, ValueError):
        print(f"  FTSE-TW: bad quote payload {q.get(F_LAST)=} {q.get(F_CHG)=}.")
        return ScrapeResult(records=0, api_rows=1, parse_errors=1)

    # Anchor base to the last TW close; carry it forward through a closed
    # stretch (same taiex_ref_date), else seed from this session's previous
    # settlement (last - change = the last TW trading day's close).
    ftse_base = _carried_base(taiex_ref_date)
    if ftse_base is None:
        ftse_base = ftse_now - chg
    if not ftse_base:
        print("  FTSE-TW: cannot determine base price.")
        return ScrapeResult(records=0, api_rows=1, parse_errors=1)

    pct = ftse_now / ftse_base - 1.0
    theoretical = taiex_ref_close * (1.0 + pct)

    record = {
        "trade_date":        trade_date,
        "front_contract":    FRONT_CONTRACT,
        "ftse_now":          round(ftse_now, 2),
        "ftse_base":         round(ftse_base, 2),
        "pct_change":        round(pct, 6),
        "taiex_ref_date":    taiex_ref_date,
        "taiex_ref_close":   round(taiex_ref_close, 2),
        "theoretical_taiex": round(theoretical, 2),
        "captured_at":       _epoch_to_utc(q.get(F_TIME)),
    }
    record.update(_fetch_txf())  # 台指期夜盤 comparison (best-effort)
    _save(record)

    txf_str = f", TXF night={record['txf_now']}" if record.get("txf_now") is not None else ""
    print(
        f"  FTSE-TW: {FRONT_CONTRACT} now={ftse_now:.2f} base={ftse_base:.2f} "
        f"({pct:+.2%}) → theoretical TAIEX {theoretical:.2f} "
        f"(ref {taiex_ref_close:.2f} @ {taiex_ref_date}){txf_str}"
    )
    return ScrapeResult(records=1, api_rows=1, parse_errors=0)


if __name__ == "__main__":
    scrape_date(date.today())
