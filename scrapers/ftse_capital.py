"""富台 (SGX FTSE Taiwan) live quote via Capital Securities SKOSQuoteLib (海期).

Replaces the cnyes 富台 feed, which stopped carrying the SGX overnight session
~2026-06-30 (the continuous quote froze at the prior day-session close pre-open
and only revived at the 08:45 SGX day open). Capital's overseas quote serves
TWN0000 — the SGX FTSE Taiwan continuous front-month, auto-rolling to the near
month — including the 15:00–05:00 night session, so the pre-open estimate is a
live 夜盤 settle again instead of a frozen stale price.

Windows-only: needs a registered x64 SKCOM.dll + comtypes + a COM STA. Imports
of comtypes are lazy inside fetch_ftse so this module imports cleanly on any
host (mirrors microtaiex broker.capital_skcom).

Sequence (all verified live 2026-07-10 against TWN0000):
  register SKReplyLib.OnReplyMessage(-> -1)   # else login warns 2017 and every
                                              # overseas call fails SK_ERROR_LOGIN_FIRST
  SetAuthority(0=正式) -> SKCenterLib_Login
  SKOSQuoteLib_Initialize -> EnterMonitorLONG -> wait OnConnect(nKind=3001)
  RequestStocks(0, "SGX,TWN0000")             # 必帶交易所前綴, else rc=3023
  GetStockByNoLONG("SGX,TWN0000", SKFOREIGNLONG())  # price = field / 10**sDecimal

SGX sessions and what carries the night move (verified 2026-07-11):
  T   session 08:45–13:45 TPE — in step with the TW cash session
  T+1 session 14:15–05:15 TPE (next morning) — the 夜盤, tracking US hours;
      SGX books it under the NEXT trading day (so Fri night is part of Mon).
  The daily K's close is therefore the T-session close (== that day's settlement
  == nRef the next trading day) and does NOT contain the night that follows it.
  The night settle only ever shows up in the snapshot's nClose (last trade).

Field mapping vs the old cnyes quote:
  nClose (最後成交)  -> ftse_now   (between 05:15 and 08:45 this IS the 夜盤 close)
  nRef   (前結算)    -> base seed  (== cnyes  last - change  == last TW settlement)
  daily K            -> bar_date/bar_close, recorded for 未開盤 diagnosis only
"""
from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

SYMBOL = "SGX,TWN0000"     # SGX FTSE Taiwan continuous front-month (auto-rolls)
_OS_READY_KIND = 3001      # overseas OnConnect ready code (國內 SKQuoteLib is 3003)
_CONNECT_TIMEOUT_S = 20.0
_QUOTE_SETTLE_S = 5.0      # pump window after RequestStocks for the snapshot to fill
_KLINE_SETTLE_S = 6.0      # pump window after RequestKLine for OnKLineData to arrive
_KLINE_KEEP = 30           # recent daily bars kept — covers the longest TW holiday
_TPE_TZ = timezone(timedelta(hours=8))
# Between the T+1 close (05:15) and the T open (08:45) nothing trades, so nClose
# is exactly the 夜盤 settle. Outside that gap nClose is a live in-session price
# and must not be persisted (it would pollute the pre-open estimate). Margins
# keep the scheduled 08:00 run comfortably inside.
_NIGHT_WIN_START = dt_time(5, 20)
_NIGHT_WIN_END = dt_time(8, 40)


@dataclass
class FtseQuote:
    now: float                  # 最後成交 (nClose) — 夜盤 settle inside the gap
    ref: float                  # 前結算 (nRef) — base anchor when no carried base
    name: str                   # e.g. 富時台指2607
    contract_month: str         # parsed "2607"
    trading_day: int            # nTradingDay (SGX trading day, night → next day)
    stale_contract: bool        # True if the continuous alias failed to roll
    bar_date: Optional[date]    # newest daily-K date (T-session close, no night)
    bar_close: Optional[float]  # newest daily-K close
    recent_closes: dict         # {date: close} for the last _KLINE_KEEP days —
                                # a K close IS that day's T-session/TW-cash close,
                                # so it is the exact base for any TAIEX ref date


def in_quiet_gap(now: Optional[datetime] = None) -> bool:
    """True inside the 05:20–08:40 TPE gap where no SGX session is running, so a
    last-trade price is the settled 夜盤 rather than a live in-session tick."""
    t = (now or datetime.now(_TPE_TZ)).time()
    return _NIGHT_WIN_START <= t <= _NIGHT_WIN_END


def _parse_month(name: str) -> str:
    """富時台指2607 -> '2607'."""
    m = re.search(r"(\d{4})", name or "")
    return m.group(1) if m else ""


def _contract_expired(month: str, today: date) -> bool:
    """Safety net for the TWN0000 auto-roll assumption: flag when the alias's
    contract month is already fully in the past (year-month < today's), i.e. it
    failed to roll and now points at a settled contract. Conservative — only
    trips once the calendar has moved past the whole month, never on a normal
    in-month near contract."""
    if not month or len(month) != 4:
        return False
    try:
        yy, mm = 2000 + int(month[:2]), int(month[2:])
    except ValueError:
        return False
    return (yy, mm) < (today.year, today.month)


def fetch_ftse(today: Optional[date] = None) -> Optional[FtseQuote]:
    """One-shot login -> subscribe -> snapshot -> disconnect.

    Returns None on any failure (the caller then treats the 富台 leg as
    unavailable and keeps the TXF leg — see scrapers.ftse_taiwan.scrape_date).
    Each call is self-contained (fresh login), so it is safe to run once per
    scheduled process."""
    today = today or date.today()

    from dotenv import load_dotenv
    load_dotenv()  # BROKER_ID/BROKER_PWD/SKCOM_DLL — cnyes path needed none

    try:
        import comtypes.client as cc
        import pythoncom
        try:
            import comtypes.gen.SKCOMLib as sk
        except ImportError:
            cc.GetModule(os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"))
            import comtypes.gen.SKCOMLib as sk
    except Exception as e:  # noqa: BLE001 - non-Windows / SDK-less host
        log.warning("ftse_capital: SKCOM/comtypes unavailable: %s", e)
        return None

    broker_id = os.environ.get("BROKER_ID")
    broker_pwd = os.environ.get("BROKER_PWD")
    if not broker_id or not broker_pwd:
        log.warning("ftse_capital: BROKER_ID/BROKER_PWD not set")
        return None

    center = cc.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)

    def msg(n: int) -> str:
        try:
            return center.SKCenterLib_GetReturnCodeMessage(n)
        except Exception:  # noqa: BLE001
            return ""

    # SKReplyLib.OnReplyMessage must be registered BEFORE login, else login
    # warns 2017 and overseas calls fail with SK_ERROR_LOGIN_FIRST.
    reply = cc.CreateObject(sk.SKReplyLib, interface=sk.ISKReplyLib)

    class _ReplyEv:
        def OnReplyMessage(self, bstrUserID, bstrMessage):  # noqa: N802
            return -1  # required sentinel per official sample

    reply_conn = cc.GetEvents(reply, _ReplyEv())  # noqa: F841 - keep ref alive

    osq = cc.CreateObject(sk.SKOSQuoteLib, interface=sk.ISKOSQuoteLib)
    ready = {"ok": False}
    klines: list = []

    class _OSEv:
        def OnConnect(self, nKind, nCode):  # noqa: N802
            if nKind == _OS_READY_KIND and nCode == 0:
                ready["ok"] = True

        def OnNotifyQuoteLONG(self, *args):  # noqa: N802 - arity varies by build
            pass

        def OnKLineData(self, *args):  # noqa: N802 - (code, "YYYY/MM/DD, O,H,L,C,V")
            klines.append(args)

    os_conn = cc.GetEvents(osq, _OSEv())  # noqa: F841 - keep ref alive

    def pump(sec: float) -> None:
        end = time.monotonic() + sec
        while time.monotonic() < end:
            pythoncom.PumpWaitingMessages()
            time.sleep(0.02)

    try:
        center.SKCenterLib_SetAuthority(0)  # 0 = 正式
        n = center.SKCenterLib_Login(broker_id, broker_pwd)
        if n != 0:
            log.warning("ftse_capital: login failed nCode=%s %s", n, msg(n))
            return None
        if osq.SKOSQuoteLib_Initialize() != 0:
            log.warning("ftse_capital: SKOSQuoteLib_Initialize failed")
            return None
        if osq.SKOSQuoteLib_EnterMonitorLONG() != 0:
            log.warning("ftse_capital: EnterMonitorLONG failed")
            return None

        deadline = time.monotonic() + _CONNECT_TIMEOUT_S
        while not ready["ok"] and time.monotonic() < deadline:
            pump(0.3)
        if not ready["ok"]:
            log.warning("ftse_capital: overseas quote server not ready (no OnConnect 3001)")
            return None

        page, rc = osq.SKOSQuoteLib_RequestStocks(0, SYMBOL)
        if rc != 0:
            log.warning("ftse_capital: RequestStocks(%s) rc=%s", SYMBOL, rc)
            return None
        pump(_QUOTE_SETTLE_S)

        st = sk.SKFOREIGNLONG()
        ret = osq.SKOSQuoteLib_GetStockByNoLONG(SYMBOL, st)
        try:
            stock, nc = ret
        except (TypeError, ValueError):
            stock, nc = st, ret
        if nc != 0 or stock is None or not stock.bstrStockNo:
            log.warning("ftse_capital: GetStockByNoLONG nCode=%s", nc)
            return None

        dec = stock.sDecimal or 0
        scale = 10 ** dec
        ref = stock.nRef / scale
        now = stock.nClose / scale
        if now <= 0:
            log.warning("ftse_capital: no last price (nClose=0)")
            return None

        name = stock.bstrStockName or ""
        month = _parse_month(name)
        stale = _contract_expired(month, today)
        if stale:
            log.warning("ftse_capital: TWN0000 points at %s past expiry — "
                        "continuous roll may have failed", month)

        # Daily K. A bar's close is the T-session close — the same instant as the
        # TW cash close — so it is the base 富台 price for that trading day; the
        # night that follows is booked into the NEXT bar. Best-effort: a K failure
        # must not sink the quote. KType=1 = daily; OnKLineData rows are
        # "code, 'YYYY/MM/DD, O,H,L,C,V'".
        closes: dict = {}
        klines.clear()
        krc = osq.SKOSQuoteLib_RequestKLine(SYMBOL, 1)
        if krc != 0:
            log.warning("ftse_capital: RequestKLine rc=%s", krc)
        else:
            pump(_KLINE_SETTLE_S)
            for krow in klines[-_KLINE_KEEP:]:
                try:
                    parts = [p.strip() for p in str(krow[1]).split(",")]
                    closes[datetime.strptime(parts[0], "%Y/%m/%d").date()] = round(
                        float(parts[4]), 2)
                except (IndexError, ValueError):
                    continue
            if not closes:
                log.warning("ftse_capital: no usable K-line rows")

        bar_date = max(closes) if closes else None
        return FtseQuote(
            now=round(now, 2), ref=round(ref, 2), name=name,
            contract_month=month, trading_day=int(stock.nTradingDay or 0),
            stale_contract=stale, bar_date=bar_date,
            bar_close=closes.get(bar_date) if bar_date else None,
            recent_closes=closes,
        )
    finally:
        try:
            osq.SKOSQuoteLib_LeaveMonitor()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    q = fetch_ftse()
    print(q)
