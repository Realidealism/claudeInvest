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

Field mapping vs the old cnyes quote:
  nClose (即時價)  -> ftse_now
  nRef   (前結算)  -> base seed  (== cnyes  last - change  == last TW settlement)
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
_TPE_TZ = timezone(timedelta(hours=8))
_SETTLE_GUARD_H = 6        # a 富台 day D's night closes D+1 05:00 TPE; treat a bar
                           # as settled only past D+1 06:00 TPE (skip the live bar)


@dataclass
class FtseQuote:
    now: float             # 即時價 (nClose)
    ref: float             # 前結算 (nRef) — base anchor when no carried base exists
    name: str              # e.g. 富時台指2607
    contract_month: str    # parsed "2607"
    trading_day: int       # nTradingDay, e.g. 20260713
    stale_contract: bool   # True if the continuous alias failed to roll (past expiry)
    bar_date: date         # newest daily-K bar date — drives 未開盤 detection
    bar_close: float       # newest daily-K close (夜盤 settle pre-open)


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

        name = stock.bstrStockName or ""
        month = _parse_month(name)
        stale = _contract_expired(month, today)
        if stale:
            log.warning("ftse_capital: TWN0000 points at %s past expiry — "
                        "continuous roll may have failed", month)

        # Daily K-line — use the newest *settled* bar, never the in-progress one.
        # A 富台 trading day D's night session closes at D+1 05:00 TPE; a bar only
        # counts once now is past D+1 06:00 TPE. So a mid-session run uses the
        # prior settled night close instead of a half-formed current-day bar —
        # no pollution, no manual seeding. KType=1 = daily; OnKLineData rows are
        # "code, 'YYYY/MM/DD, O,H,L,C,V'".
        klines.clear()
        krc = osq.SKOSQuoteLib_RequestKLine(SYMBOL, 1)
        if krc != 0:
            log.warning("ftse_capital: RequestKLine rc=%s", krc)
            return None
        pump(_KLINE_SETTLE_S)
        if not klines:
            log.warning("ftse_capital: no K-line data returned")
            return None
        now_tpe = datetime.now(_TPE_TZ)
        settled = None
        for krow in reversed(klines):
            try:
                parts = [p.strip() for p in str(krow[1]).split(",")]
                bd = datetime.strptime(parts[0], "%Y/%m/%d").date()
                bc = float(parts[4])
            except (IndexError, ValueError):
                continue
            settle_after = datetime.combine(
                bd + timedelta(days=1), dt_time(_SETTLE_GUARD_H, 0), _TPE_TZ)
            if now_tpe >= settle_after:
                settled = (bd, bc)
                break
        if settled is None:
            log.warning("ftse_capital: no settled daily K yet")
            return None
        bar_date, bar_close = settled
        now = bar_close  # settled night-session close, not the live in-day price
        if now <= 0:
            log.warning("ftse_capital: no price (K close=0)")
            return None

        return FtseQuote(
            now=round(now, 2), ref=round(ref, 2), name=name,
            contract_month=month, trading_day=int(stock.nTradingDay or 0),
            stale_contract=stale, bar_date=bar_date, bar_close=round(bar_close, 2),
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
