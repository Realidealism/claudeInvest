"""Capital Securities SKCOM adapter (COM/ActiveX, Windows) -- primary broker.

LIVE implementation grounded in the official Capital "策略王" Python example
(CapitalAPI 2.13.58, PythonExampleV2). It CANNOT run on a machine without
SKCOM.dll registered + comtypes + a Windows COM apartment, so every method
imports comtypes lazily: the module still imports fine (and the testable core
stays green) on a machine without the SDK.

Confirmed against the official example/doc:
- Objects/sinks: CreateObject(...interface=...) + comtypes.client.GetEvents.
- Env: SKCenterLib_SetAuthority(0=正式, 2=測試) BEFORE login.
- Login: SKCenterLib_Login(id, pwd) -> nCode (0 = success).
- Quote: SKQuoteLib_EnterMonitorLONG(); OnConnection(nKind=3003, nCode=0)=ready;
  psPageNo,nCode = SKQuoteLib_RequestTicks(page, symbol);
  OnNotifyTicksLONG(sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros,
                    nBid, nAsk, nClose, nQty, nSimulate); prices are value/100.0.
- Order: SKOrderLib_Initialize -> ReadCertByID -> GetUserAccount;
  bstrMessage,nCode = SendFutureOrderCLR(user, bAsync, FUTUREORDER).
- Reply: SKReplyLib_ConnectByID(user); OnNewData(user, csv) -> Type=='D' is 成交.
- Positions: GetOpenInterestGW(user, account, 1) -> OnOpenInterest(csv) event.

Items still needing on-site confirmation are marked ``TODO(verify)`` -- chiefly
the exact OnNewData / OnOpenInterest column indices (the handlers log the raw
split so the first real fill makes them trivial to lock down).
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional, Tuple

from .base import BrokerAdapter, ReconnectMixin
from .types import (
    Bar,
    ConnectionStatus,
    OpenClose,
    OrderRequest,
    OrderResult,
    Position,
    Side,
    Tick,
    TimeInForce,
    Trade,
)

log = logging.getLogger(__name__)

# Quote OnConnection nKind: 3001=連線成功, 3002=斷線, 3003=商品報價就緒(可查詢).
# Queries (RequestKLine/RequestTicks) require STOCKS_READY = 3003.
_NKIND_STOCKS_READY = 3003
# Disconnect / connection-error codes -> trigger reconnect. TODO(verify) for your version
# (3002 disconnect; 3022 SOLCLIENTAPI_FAIL; 3033 SOLACE_SESSION_EVENT_ERROR from doc table).
_NKIND_LOST = (3002, 3022, 3033)
# Quote tick prices come as integers scaled x100 (official sample divides by 100).
_QUOTE_PRICE_SCALE = 100.0

# FUTUREORDER value encodings (official TFOrder example).
_BUYSELL = {Side.BUY: 0, Side.SELL: 1}
_TRADETYPE = {TimeInForce.ROD: 0, TimeInForce.IOC: 1, TimeInForce.FOK: 2}
_NEWCLOSE = {OpenClose.OPEN: 0, OpenClose.COVER: 1, OpenClose.AUTO: 2}

# OnNewData comma-field indices for 國內期 (TF). Field ORDER is from the official
# doc (12.回報); the BuySell group expands to 5 columns for futures:
#   [6]BuySell [7]新平倉/當沖 [8]委託條件 [9]價格類別 [10]N/A
# TODO(verify): confirm these against a printed fill; handler logs raw split.
_RPT_MARKET = 1     # TS/TF/TO/OF...
_RPT_TYPE = 2       # N委託 C取消 U改量 P改價 D成交 B改價改量 S動態退單
_RPT_BUYSELL = 6    # B買 / S賣
_RPT_COMID = 12     # 商品代碼
_RPT_PRICE = 15     # Price (for D=成交 是成交價)
_RPT_QTY = 24       # Qty (口數)


class CapitalSKCOMAdapter(BrokerAdapter, ReconnectMixin):
    def __init__(
        self,
        user_id: str,
        password: str,
        full_account: str,           # futures account: "broker_code-account"
        cert_id: Optional[str] = None,
        skcom_dll_path: str = "SKCOM.dll",   # x64 SKCOM.dll path, or registered name
        test_env: bool = False,      # True => SetAuthority(2) 測試環境 (UAT)
        **kwargs,
    ) -> None:
        super().__init__()
        self._init_reconnect()
        self._id = user_id
        self._pwd = password
        self._account = full_account
        self._cert_id = cert_id or user_id
        self._dll_path = skcom_dll_path
        self._test_env = test_env
        # COM modules / objects (filled lazily in _init_com)
        self._cc = None              # comtypes.client
        self._pythoncom = None
        self._sk = None              # comtypes.gen.SKCOMLib
        self._center = None
        self._quote = None
        self._order = None
        self._reply = None
        self._ev_quote = None        # event sink holders (keep refs to avoid GC)
        self._ev_reply = None
        self._ev_order = None
        self._pump_stop = threading.Event()
        self._pump: Optional[threading.Thread] = None
        self._kbar_buffer: List[Bar] = []
        self._kbar_done = threading.Event()
        self._kbar_tf = "1m"
        self._quote_ready = threading.Event()  # set on OnConnection(3003)
        self._commodity_lines: List[str] = []  # OnNotifyCommodityListWithTypeNo buffer
        self._serve_stop = threading.Event()   # stops the serve() pump loop
        self._quote_lost = threading.Event()   # set on OnConnection disconnect/error
        # open-interest query buffering (GetOpenInterestGW -> OnOpenInterest)
        self._oi_lines: List[str] = []
        self._oi_done = threading.Event()

    # ---- connection ----
    def connect(self, quote_only: bool = False, background_pump: bool = True) -> None:
        # quote_only: skip order init + reply (history fetch needs quotes only).
        # background_pump=False: do NOT spawn a pump thread -- the caller's thread
        #   pumps COM messages inline (required for STA event delivery in one-shot
        #   tools, since events arrive on the thread that created the objects).
        self._emit_connection(ConnectionStatus.CONNECTING)
        self._init_com()
        self._set_authority()
        self._login()
        if not quote_only:
            self._init_order()
        self._connect_quote()
        if not quote_only:
            self._connect_reply()
        if background_pump:
            self._start_pump()
            self._start_watchdog()
        log.info("Capital SKCOM connect (quote_only=%s, bg_pump=%s), account=%s test_env=%s",
                 quote_only, background_pump, self._account, self._test_env)

    def disconnect(self) -> None:
        self._stop_watchdog()
        self._pump_stop.set()
        self._serve_stop.set()
        if self._pump is not None:
            self._pump.join(timeout=2.0)
        self._emit_connection(ConnectionStatus.DISCONNECTED)

    def serve(self, symbols, periodic=None, period: float = 1.0, with_orders: bool = True,
              on_ready=None) -> None:
        """Run the live loop on THE CALLING THREAD: connect, subscribe, pump forever.

        All four COM objects are created AND message-pumped on this one thread
        (STA), so quote/report events are delivered here and any order placed
        from within an event handler is a same-thread call -- no marshaling, no
        background pump. Blocks until disconnect()/stop or KeyboardInterrupt.

        symbols: contract codes to subscribe (e.g. ["TM0000"]).
        periodic: optional callable invoked roughly every ``period`` seconds
            (for heartbeat / session checks). No auto-reconnect in this mode.
        on_ready: optional callable invoked ONCE after the quote server is ready
            but before tick subscription — the place to fetch history via
            get_kbars on THIS single login (no second login). Its failure is
            logged and swallowed so warmup never kills the live loop.
        """
        import time as _time

        self._emit_connection(ConnectionStatus.CONNECTING)
        self._init_com()
        self._set_authority()
        self._login()
        if with_orders:
            self._init_order()
        self._connect_quote()
        if with_orders:
            self._connect_reply()
        if not self._pump_wait(self._quote_ready, 20.0):
            raise RuntimeError("quote server not ready (no OnConnection 3003)")
        if on_ready is not None:
            try:
                on_ready()
            except Exception:  # noqa: BLE001 - warmup must never kill the live loop
                log.exception("on_ready hook failed (continuing without warmup)")
        for sym in symbols:
            self.subscribe(sym)

        self._serve_stop.clear()
        self._quote_lost.clear()
        last_periodic = 0.0
        while not self._serve_stop.is_set():
            self._pythoncom.PumpWaitingMessages()
            if self._quote_lost.is_set():
                # In-loop re-EnterMonitor does NOT reliably resume tick flow (verified
                # live: after a 3002 disconnect it gets 3001 but ticks stay frozen).
                # Exit so the supervisor (NSSM) does a clean full restart -- a fresh
                # login + EnterMonitor + RequestTicks is the only proven recovery.
                log.warning("quote connection lost (OnConnection 3002); exiting for clean restart")
                raise RuntimeError("quote connection lost; exiting for supervisor restart")
            now = _time.monotonic()
            if periodic is not None and now - last_periodic >= period:
                try:
                    periodic()
                except Exception:  # noqa: BLE001 - periodic must not kill the loop
                    log.exception("periodic hook failed")
                last_periodic = now
            _time.sleep(0.005)
        self._emit_connection(ConnectionStatus.DISCONNECTED)

    def _init_com(self) -> None:
        import comtypes.client as cc  # lazy: only available on a configured Windows host
        import pythoncom

        cc.GetModule(self._dll_path)
        import comtypes.gen.SKCOMLib as sk

        self._cc = cc
        self._pythoncom = pythoncom
        self._sk = sk
        self._center = cc.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
        self._quote = cc.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
        self._order = cc.CreateObject(sk.SKOrderLib, interface=sk.ISKOrderLib)
        self._reply = cc.CreateObject(sk.SKReplyLib, interface=sk.ISKReplyLib)
        # Bind event sinks to this adapter (its On* methods receive the events).
        # OnReplyMessage must be registered before login (doc 3.登入); done here.
        self._ev_quote = cc.GetEvents(self._quote, self)
        self._ev_reply = cc.GetEvents(self._reply, self)
        self._ev_order = cc.GetEvents(self._order, self)

    def _msg(self, n_code: int) -> str:
        try:
            return self._center.SKCenterLib_GetReturnCodeMessage(n_code)
        except Exception:  # noqa: BLE001
            return str(n_code)

    def _set_authority(self) -> None:
        # 0=正式環境, 1=正式SGX, 2=測試環境, 3=測試SGX (official sample).
        flag = 2 if self._test_env else 0
        n_code = self._center.SKCenterLib_SetAuthority(flag)
        if n_code != 0:
            log.warning("SetAuthority(%s) -> %s %s", flag, n_code, self._msg(n_code))

    def _login(self) -> None:
        n_code = self._center.SKCenterLib_Login(self._id, self._pwd)
        if n_code != 0:  # TODO(verify): some accounts return a 2FA/agreement code
            raise RuntimeError(f"Capital login failed: {n_code} {self._msg(n_code)}")
        log.info("Capital login OK")

    def _init_order(self) -> None:
        n_code = self._order.SKOrderLib_Initialize()
        if n_code != 0:
            raise RuntimeError(f"SKOrderLib_Initialize failed: {n_code} {self._msg(n_code)}")
        self._order.ReadCertByID(self._cert_id)
        self._order.GetUserAccount()  # accounts arrive via OnAccount

    def _connect_quote(self) -> None:
        self._quote_ready.clear()
        n_code = self._quote.SKQuoteLib_EnterMonitorLONG()
        if n_code != 0:
            raise RuntimeError(f"EnterMonitorLONG failed: {n_code} {self._msg(n_code)}")

    def _connect_reply(self) -> None:
        n_code = self._reply.SKReplyLib_ConnectByID(self._id)
        if n_code != 0:
            raise RuntimeError(f"SKReplyLib_ConnectByID failed: {n_code} {self._msg(n_code)}")

    def _reconnect(self) -> None:
        self._init_com()
        self._set_authority()
        self._login()
        self._init_order()
        self._connect_quote()
        self._connect_reply()
        self._start_pump()

    def _start_pump(self) -> None:
        self._pump_stop.clear()
        self._pump = threading.Thread(
            target=self._pump_loop, name="skcom-message-pump", daemon=True
        )
        self._pump.start()

    def _pump_loop(self) -> None:
        self._pythoncom.CoInitialize()
        try:
            while not self._pump_stop.wait(0.01):
                self._pythoncom.PumpWaitingMessages()
        finally:
            self._pythoncom.CoUninitialize()

    def _pump_wait(self, event: threading.Event, timeout: float) -> bool:
        """Pump COM messages on the CALLING thread until ``event`` is set.

        Used when there is no background pump thread (one-shot tools): COM (STA)
        delivers events to the thread that created the objects, so that thread
        must pump for OnConnection / OnNotifyKLineData to fire.
        """
        import time as _time
        deadline = _time.monotonic() + timeout
        while not event.is_set() and _time.monotonic() < deadline:
            self._pythoncom.PumpWaitingMessages()
            _time.sleep(0.01)
        return event.is_set()

    # ---- quotes ----
    def _do_subscribe(self, symbol: str) -> None:
        page, n_code = self._quote.SKQuoteLib_RequestTicks(0, symbol)
        if n_code != 0:
            log.warning("RequestTicks %s failed: %s %s", symbol, n_code, self._msg(n_code))
        else:
            log.info("(SKCOM) RequestTicks %s page=%s", symbol, page)

    def OnNotifyCommodityListWithTypeNo(self, sMarketNo, bstrCommodityData) -> None:  # noqa: N802
        self._mark_alive()
        self._commodity_lines.append(bstrCommodityData)

    def list_commodities(self, market: int, wait: float = 6.0) -> List[str]:
        """Request the product list for a market (2=期貨) and collect the raw
        OnNotifyCommodityListWithTypeNo payloads (no explicit complete event)."""
        import time as _time
        self._commodity_lines.clear()
        if self._pump is None:
            self._pump_wait(self._quote_ready, 20.0)
        else:
            self._quote_ready.wait(timeout=20.0)
        self._quote.SKQuoteLib_RequestStockList(market)
        deadline = _time.monotonic() + wait
        while _time.monotonic() < deadline:
            if self._pump is None:
                self._pythoncom.PumpWaitingMessages()
            _time.sleep(0.02)
        return list(self._commodity_lines)

    def stock_info(self, symbol: str):
        """Look up a symbol via GetStockByNoLONG. Returns (nCode, name, decimal).

        nCode == 0 means the symbol is valid on the quote server. Requires the
        quote server ready (waits for OnConnection 3003 if pumping inline).
        """
        if self._pump is None:
            self._pump_wait(self._quote_ready, 20.0)
        else:
            self._quote_ready.wait(timeout=20.0)
        stock, n_code = self._quote.SKQuoteLib_GetStockByNoLONG(symbol, self._sk.SKSTOCKLONG())
        if n_code == 0 and stock is not None:
            return n_code, stock.bstrStockName, stock.sDecimal
        return n_code, None, None

    def _resolve_symbol(self, market: int, index: int) -> str:
        # Map (market, index) -> contract code via GetStockByIndexLONG.
        try:
            stock, n_code = self._quote.SKQuoteLib_GetStockByIndexLONG(
                market, index, self._sk.SKSTOCKLONG()
            )
            if n_code == 0 and stock is not None:
                return stock.bstrStockNo
        except Exception:  # noqa: BLE001
            pass
        return f"{market}:{index}"

    def OnNotifyTicksLONG(  # noqa: N802 (COM event naming)
        self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros,
        nBid, nAsk, nClose, nQty, nSimulate,
    ) -> None:
        self._mark_alive()
        if nSimulate != 0:
            return  # skip trial-calculation ticks; only act on real ticks
        from datetime import datetime

        symbol = self._resolve_symbol(sMarketNo, nIndex)
        d, t = int(nDate), int(nTimehms)
        ts = datetime(d // 10000, (d // 100) % 100, d % 100,
                      t // 10000, (t // 100) % 100, t % 100)
        self._emit_tick(Tick(
            symbol=symbol,
            ts=ts,
            price=nClose / _QUOTE_PRICE_SCALE,
            volume=int(nQty),
            bid=nBid / _QUOTE_PRICE_SCALE,
            ask=nAsk / _QUOTE_PRICE_SCALE,
        ))

    def OnConnection(self, nKind, nCode) -> None:  # noqa: N802 (quote server state)
        self._mark_alive()
        log.info("OnConnection nKind=%s nCode=%s", nKind, nCode)
        if nCode == 0 and nKind == _NKIND_STOCKS_READY:
            self._quote_ready.set()
            self._quote_lost.clear()
            self._emit_connection(ConnectionStatus.CONNECTED)
        elif nKind in _NKIND_LOST:
            self._quote_ready.clear()
            self._quote_lost.set()
            self._emit_connection(ConnectionStatus.RECONNECTING)

    def OnNotifyKLineData(self, bstrStockNo, bstrData) -> None:  # noqa: N802
        # 新版格式 (sOutType=1, prices already decimal-scaled):
        #   "年/月/日, 開, 高, 低, 收, 量"  或分線 "年/月/日 時:分, 開,高,低,收,量"
        if bstrData.startswith("##"):   # terminator (also see OnKLineComplete)
            log.info("KLine terminator: %r", bstrData)
            self._kbar_done.set()
            return
        from datetime import datetime
        if not self._kbar_buffer:
            log.info("first K row raw: %r", bstrData)
        parts = [p.strip() for p in bstrData.split(",")]
        try:
            stamp = parts[0]
            ts = (datetime.strptime(stamp, "%Y/%m/%d %H:%M") if ":" in stamp
                  else datetime.strptime(stamp, "%Y/%m/%d"))
            o, h, l, c = (float(parts[i]) for i in range(1, 5))
            vol = int(float(parts[5]))
        except (ValueError, IndexError):
            log.warning("unparsed K row: %r", bstrData)
            return
        self._kbar_buffer.append(Bar(bstrStockNo, ts, o, h, l, c, vol, self._kbar_tf))

    def OnKLineComplete(self, bstrEndString) -> None:  # noqa: N802
        # Fired once all history rows have been delivered (bstrEndString starts "##").
        log.info("OnKLineComplete: %r", bstrEndString)
        self._kbar_done.set()

    # ---- orders ----
    def place_order(self, req: OrderRequest) -> OrderResult:
        order = self._to_capital_order(req)
        message, n_code = self._order.SendFutureOrderCLR(self._id, False, order)  # bAsync=False
        accepted = (n_code == 0)
        return OrderResult(order_id=str(message), accepted=accepted,
                           msg=self._msg(n_code), raw=(message, n_code))

    def update_order(self, order_id, price=None, lot=None) -> OrderResult:
        raise NotImplementedError("SKCOM futures amend not wired -- TODO(verify) DecreaseOrder API")

    def cancel_order(self, order_id) -> OrderResult:
        raise NotImplementedError("SKCOM futures cancel not wired -- TODO(verify) CancelOrderByBookNo")

    def _to_capital_order(self, req: OrderRequest):
        sk = self._sk
        o = sk.FUTUREORDER()
        o.bstrFullAccount = self._account
        o.bstrStockNo = req.symbol
        o.sBuySell = _BUYSELL[req.side]
        o.sTradeType = _TRADETYPE[req.tif]
        o.sNewClose = _NEWCLOSE[req.open_close]
        o.nQty = req.lot
        # Futures has no market-price flag in this struct; pass the limit price.
        # Our engine always supplies a price (signal bar close); guard None.
        if req.price is None:
            raise ValueError("Capital futures order requires a limit price")
        o.bstrPrice = str(req.price)
        o.sReserved = 0          # 0 盤中(T盤及T+1盤), 1 T盤預約
        o.sDayTrade = 0          # 0 否, 1 當沖
        return o

    def OnNewData(self, bstrUserID, bstrData) -> None:  # noqa: N802
        self._mark_alive()
        cols = bstrData.split(",")
        # Log the enumerated split so column indices can be confirmed on first fill.
        log.debug("OnNewData split: %s", list(enumerate(cols)))
        try:
            if cols[_RPT_TYPE] != "D":   # D = 成交 (fill); others are ack/cancel/amend
                return
            side = Side.BUY if cols[_RPT_BUYSELL] == "B" else Side.SELL
            symbol = cols[_RPT_COMID]
            price = float(cols[_RPT_PRICE])
            qty = int(cols[_RPT_QTY]) if cols[_RPT_QTY].strip() else 1
        except (IndexError, ValueError):
            log.warning("unparsed reply: %r", bstrData)
            return
        from datetime import datetime
        self._emit_trade(Trade(order_id="", symbol=symbol, side=side,
                               price=price, lot=qty, ts=datetime.now()))

    # ReplyLib events
    def OnReplyMessage(self, bstrUserID, bstrMessage) -> int:  # noqa: N802
        # Must be present + return a confirm code for login to proceed (doc 3.登入).
        return -1

    def OnComplete(self, bstrUserID) -> None:  # noqa: N802
        self._mark_alive()
        log.info("reply backfill complete: %s", bstrUserID)

    def OnSolaceReplyConnection(self, bstrUserID, nErrorCode) -> None:  # noqa: N802
        self._mark_alive()

    def OnSolaceReplyDisconnect(self, bstrUserID, nErrorCode) -> None:  # noqa: N802
        self._emit_connection(ConnectionStatus.RECONNECTING)

    # OrderLib events
    def OnAccount(self, bstrLogInID, bstrAccountData) -> None:  # noqa: N802
        # values[1]=broker(IB) + values[3]=account -> full account string.
        log.info("OnAccount: %s", bstrAccountData)

    def OnAsyncOrder(self, nThreadID, nCode, bstrMessage) -> None:  # noqa: N802
        self._mark_alive()

    def OnOpenInterest(self, bstrData) -> None:  # noqa: N802
        # Async result of GetOpenInterestGW; accumulate then signal completion.
        self._mark_alive()
        if bstrData:
            self._oi_lines.append(bstrData)
        self._oi_done.set()

    # ---- account ----
    def list_positions(self) -> List[Position]:
        self._oi_lines.clear()
        self._oi_done.clear()
        n_code = self._order.GetOpenInterestGW(self._id, self._account, 1)
        if n_code != 0:
            raise RuntimeError(f"GetOpenInterestGW failed: {n_code} {self._msg(n_code)}")
        self._oi_done.wait(timeout=10.0)
        # TODO(verify): exact OnOpenInterest column layout (symbol/side/qty/avg);
        # the raw lines are buffered in self._oi_lines for confirmation.
        positions: List[Position] = []
        for line in self._oi_lines:
            cols = line.split(",")
            log.debug("OnOpenInterest split: %s", list(enumerate(cols)))
        return positions

    # ---- history (history-only; needs a futures account for TF/TO products) ----
    def get_kbars(self, symbol: str, start: str, end: str, *,
                  minute: int = 5, session: int = 0, kline_type: int = 0) -> List[Bar]:
        """Fetch historical K bars. start/end are 'YYYYMMDD'.

        kline_type: 0 = 分線 (minute), 4 = 日線 (daily). minute = N-minute K when
        kline_type=0. session: 0=全盤(含日夜盤), 1=AM盤. Prices come decimal-scaled;
        rows arrive via OnNotifyKLineData, OnKLineComplete signals the end.
        """
        inline = self._pump is None   # no background pump -> pump on this thread
        ready = (self._pump_wait(self._quote_ready, 20.0) if inline
                 else self._quote_ready.wait(timeout=20.0))
        if not ready:
            raise RuntimeError("quote server not ready (no OnConnection 3003); "
                               "check EnterMonitorLONG / network / market hours")
        self._kbar_buffer.clear()
        self._kbar_done.clear()
        self._kbar_tf = "1d" if kline_type == 4 else f"{minute}m"
        n_code = self._quote.SKQuoteLib_RequestKLineAMByDate(
            symbol, kline_type, 1, session, start, end, minute,
        )
        if n_code != 0:
            raise RuntimeError(f"RequestKLineAMByDate failed: {n_code} {self._msg(n_code)}")
        if inline:
            self._pump_wait(self._kbar_done, 30.0)
        else:
            self._kbar_done.wait(timeout=30.0)
        return list(self._kbar_buffer)
