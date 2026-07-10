"""
富台指數 → 理論 TAIEX scraper.

Sources:
  富台 (SGX FTSE Taiwan): Capital Securities 海期 SKOSQuoteLib, symbol
    "SGX,TWN0000" (see scrapers.ftse_capital). Replaced the cnyes 富台 feed
    in 2026-07 after cnyes stopped carrying the SGX overnight session (froze
    the quote pre-open, only reviving at the 08:45 day open). The continuous
    "TWN0000" auto-rolls the front month.
  台指期夜盤 (TXF): 鉅亨網 (cnyes) public quote API, symbol TWF:TXF:FUTURES.

富台 is the SGX FTSE Taiwan Index Futures (the contract was the MSCI Taiwan /
摩台 future before SGX switched the index licence to FTSE in 2020); its quote
includes the night session (15:00–05:00 TPE, tracking US hours) — exactly when
a TW holiday's biggest moves happen.

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
from scrapers.ftse_capital import fetch_ftse
from utils.format_shift import ScrapeResult
from utils.http_client import fetch_json_retry

QUOTE_URL = "https://ws.api.cnyes.com/ws/api/v1/quote/quotes/{symbol}"
SYM_TXF = "TWF:TXF:FUTURES"     # TAIFEX 台指期大台 continuous front month (cnyes)
FRONT_CONTRACT = "TWN0000"      # SGX FTSE Taiwan continuous front month (Capital 海期)

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


def _last_ftse_bar_state() -> tuple[date | None, date | None]:
    """(ftse_bar_date, trade_date) of the latest stored row, for 未開盤 detection.

    ftse_bar_date is the last settled 富台 K date; trade_date is the calendar day
    that run happened on — used to tell a same-day rerun (bar can't advance yet)
    from a genuine no-advance across days (SGX holiday)."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT ftse_bar_date, trade_date FROM tw.ftse_taiwan "
            "WHERE ftse_bar_date IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        return None, None
    return row["ftse_bar_date"], row["trade_date"]


def _save(record: dict[str, Any]) -> None:
    sql = """
        INSERT INTO tw.ftse_taiwan
            (trade_date, front_contract, ftse_now, ftse_base, pct_change,
             taiex_ref_date, taiex_ref_close, theoretical_taiex, captured_at,
             ftse_bar_date, txf_now, txf_pct_change, txf_captured_at)
        VALUES
            (%(trade_date)s, %(front_contract)s, %(ftse_now)s, %(ftse_base)s,
             %(pct_change)s, %(taiex_ref_date)s, %(taiex_ref_close)s,
             %(theoretical_taiex)s, %(captured_at)s, %(ftse_bar_date)s,
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
            ftse_bar_date     = EXCLUDED.ftse_bar_date,
            txf_now           = EXCLUDED.txf_now,
            txf_pct_change    = EXCLUDED.txf_pct_change,
            txf_captured_at   = EXCLUDED.txf_captured_at,
            fetched_at        = NOW()
    """
    with get_cursor() as cur:
        cur.execute(sql, record)
        # Latest-only: prune older rows so the table holds a single row.
        cur.execute("DELETE FROM tw.ftse_taiwan WHERE trade_date < %s",
                    (record["trade_date"],))


def scrape_date(trade_date: date) -> tuple[ScrapeResult, bool]:
    """Fetch 富台 + TXF night quotes, convert to theoretical TAIEX, upsert.

    trade_date is the calendar day this snapshot belongs to (the row key);
    the conversion always uses the latest available 富台 price and the
    latest TAIEX close, regardless of trade_date.

    The two legs are DECOUPLED: a 富台 outage (cnyes stopped carrying the SGX
    overnight session ~2026-06-30, so the quote freezes pre-open and only
    revives at the 08:45 day-session open) leaves the 富台 columns null but no
    longer aborts the TXF leg — the TXF night-session settle is the earlier,
    more direct open estimate and must still be written so the 開盤前估值
    section is never fully blank. Returns (result, ftse_ok) where ftse_ok is
    True only when a valid, timestamped 富台 quote produced a theoretical TAIEX;
    the caller uses it to decide whether to keep retrying for a late 富台."""
    taiex = _latest_taiex()
    if taiex is None:
        print("  FTSE-TW: no TAIEX reference in tw.index_prices — skipping.")
        return ScrapeResult(records=0, api_rows=0, parse_errors=1), False
    taiex_ref_date, taiex_ref_close = taiex

    # ── 富台 leg (best-effort; null on outage) ──────────────────────────────
    ftse_fields: dict[str, Any] = {
        "ftse_now": None, "ftse_base": None, "pct_change": None,
        "theoretical_taiex": None, "captured_at": None, "ftse_bar_date": None,
    }
    ftse_ok = False
    ftse_log = "富台 → 暫無"
    last_bar, last_run = _last_ftse_bar_state()
    fq = fetch_ftse(trade_date)
    if fq is None:
        print("  FTSE-TW: Capital 富台 quote unavailable (富台 leg skipped).")
    elif fq.stale_contract:
        # Safety net for the TWN0000 continuous-alias auto-roll assumption: if
        # it points at a settled contract, skip rather than publish a stale base.
        print(f"  FTSE-TW: TWN0000 contract {fq.contract_month} past expiry — "
              f"continuous roll may have failed; 富台 leg skipped.")
    elif (last_bar is not None and fq.bar_date <= last_bar
          and last_run is not None and last_run < trade_date):
        # No newer settled K AND a new calendar day since the last run → SGX did
        # not open (holiday). A same-day rerun (last_run == trade_date) is NOT
        # 未開盤 — it just re-sees the same settled bar. Mark 未開盤: 富台 columns
        # stay null but record the (unchanged) bar_date so the frontend shows
        # 未開盤, not a stale price.
        ftse_fields["ftse_bar_date"] = fq.bar_date
        ftse_log = f"富台 → 未開盤 (K-line still {fq.bar_date})"
        print(f"  FTSE-TW: 富台 未開盤 — newest settled K-line {fq.bar_date} "
              f"<= last {last_bar} across days; SGX did not open.")
    else:
        ftse_now = fq.now
        # Anchor base to the last TW close; carry it forward through a closed
        # stretch (same taiex_ref_date), else seed from Capital's prior settle
        # (nRef == the last TW trading day's settlement == cnyes last-change).
        ftse_base = _carried_base(taiex_ref_date)
        if ftse_base is None:
            ftse_base = fq.ref
        if not ftse_base:
            print("  FTSE-TW: cannot determine base price (富台 leg skipped).")
        else:
            pct = ftse_now / ftse_base - 1.0
            theoretical = taiex_ref_close * (1.0 + pct)
            ftse_fields = {
                "ftse_now":          round(ftse_now, 2),
                "ftse_base":         round(ftse_base, 2),
                "pct_change":        round(pct, 6),
                "theoretical_taiex": round(theoretical, 2),
                "captured_at":       datetime.now(timezone.utc),
                "ftse_bar_date":     fq.bar_date,
            }
            ftse_ok = True
            ftse_log = (
                f"TWN0000({fq.contract_month}) bar {fq.bar_date} now={ftse_now:.2f} "
                f"base={ftse_base:.2f} ({pct:+.2%}) → theoretical TAIEX "
                f"{theoretical:.2f} (ref {taiex_ref_close:.2f} @ {taiex_ref_date})"
            )

    # ── TXF night leg (best-effort) ─────────────────────────────────────────
    txf = _fetch_txf()

    # Write when any leg has something to persist: a live 富台, a live TXF, or a
    # 未開盤 marker (bar_date set, 富台 null) so the page shows 未開盤 rather
    # than a stale price. Only a total failure (nothing at all) skips the write.
    ftse_marker = ftse_fields.get("ftse_bar_date") is not None
    if not ftse_ok and not ftse_marker and txf.get("txf_now") is None:
        print("  FTSE-TW: nothing to write (all legs unavailable).")
        return ScrapeResult(records=0, api_rows=1, parse_errors=0), False

    record = {
        "trade_date":      trade_date,
        "front_contract":  FRONT_CONTRACT,
        "taiex_ref_date":  taiex_ref_date,
        "taiex_ref_close": round(taiex_ref_close, 2),
        **ftse_fields,
        **txf,
    }
    _save(record)

    txf_str = f", TXF night={record['txf_now']}" if record.get("txf_now") is not None else ""
    print(f"  FTSE-TW: {ftse_log}{txf_str}")
    return ScrapeResult(records=1, api_rows=1, parse_errors=0), ftse_ok


if __name__ == "__main__":
    scrape_date(date.today())
