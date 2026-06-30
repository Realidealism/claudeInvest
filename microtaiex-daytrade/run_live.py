"""Live real-time loop against Capital SKCOM (single-threaded COM design).

RUN ON THE SKCOM MACHINE, during market hours (day 08:45-13:45 / night 15:00-05:00).
Reads .env (BROKER_ID/PWD/FUT_ACCOUNT/SKCOM_DLL). test_env is FORCED on (模擬環境).

    python run_live.py                    # observe-only (正式環境, NO orders) -- safe
    python run_live.py --trade --test-env # full engine, 模擬環境 (needs 測試帳號)
    python run_live.py --trade --real     # full engine, 正式環境 REAL orders, max_lots=1

The whole loop runs on the broker's COM thread (broker.serve): events fire there
and orders are placed from within handlers (same thread) -- no marshaling.
Observe needs no orders so it runs on 正式環境 safely. --trade refuses to place
REAL orders unless you pass --real (or --test-env for 模擬). Ctrl-C to stop.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from broker.factory import make_broker            # noqa: E402
from broker.sim import SimBroker                  # noqa: E402
from broker.types import OpenClose, Side, Tick    # noqa: E402
from core import clock                            # noqa: E402
from core.clock import Session                     # noqa: E402
from core.engine import TradingEngine             # noqa: E402
from data.bar_aggregator import BarAggregator     # noqa: E402
from data.trade_log import TradeLog               # noqa: E402
from position.state_machine import PositionStateMachine  # noqa: E402
from risk.risk_manager import RiskConfig, RiskManager    # noqa: E402
from strategy.base import StrategyContext         # noqa: E402
from strategy.strategies.composite import CompositeStrategy  # noqa: E402
from notify import TelegramNotifier                # noqa: E402

_POINT_VALUE = 10   # micro-Taiex NT$ per index point


def _side_zh(side) -> str:
    return "做多" if side is Side.BUY else "做空"


def _fmt_entry(order, reason, ts) -> str:
    emoji = "🟢" if order.side is Side.BUY else "🔴"
    sig = f"　訊號 {reason}" if reason else ""
    when = f"　{ts:%m-%d %H:%M}" if ts else ""
    return (f"{emoji} 進場 {_side_zh(order.side)} {order.symbol}\n"
            f"價格 {order.price:.0f}{sig}{when}")


def _fmt_exit(rt) -> str:
    gross = rt.points * _POINT_VALUE * rt.lot
    ok = "✅" if rt.points > 0 else "❌"
    sig = f"　訊號 {rt.reason}" if getattr(rt, "reason", "") else ""
    dur = ""
    if rt.entry_ts and rt.exit_ts:
        dur = f"　持倉 {int((rt.exit_ts - rt.entry_ts).total_seconds() // 60)}m"
    return (f"{ok} 出場 {_side_zh(rt.side)} {rt.symbol}\n"
            f"{rt.entry_price:.0f} → {rt.exit_price:.0f}　"
            f"{rt.points:+.0f} 點　{gross:+,.0f} 元{sig}{dur}")
from config_env import load_dotenv                # noqa: E402

SYMBOL = "TM0000"
LOOKBACK = 20           # pick/touch engulfing relative-low/high window
BREAKOUT_LB = 20        # buy/sell Donchian breakout window
FLEE_LB = 10            # buy_flee/sell_flee trap window
ATR_MULT = 2.0          # fallback Chandelier stop distance (both sides)
# Per-direction stop distance from the TM 2D sweep (robust across both halves):
# long-side wants room (6), short-side stays tight (2) — matches the daily
# signal-factory's buy/pick=6, touch/sell=2/1.5 on a different instrument.
LONG_ATR_MULT = 6.0
SHORT_ATR_MULT = 2.0
# Exit handicap: tolerate a breach of up to buffer*ATR before the stop fires
# (noise filter). Sweep peak at 0.05 across full + both halves.
STOP_BUFFER_ATR = 0.05
ATR_PERIOD = 21
STRAT_TF = "5m"


def main(argv) -> int:
    paper = "--paper" in argv
    trade = "--trade" in argv and not paper
    test_env = "--test-env" in argv
    real = "--real" in argv
    # --gated: long-only pick+sell_flee gated by the self-computed daily-state
    # (risk-adjusted variant). Default: ungated all-6 signals.
    gated = "--gated" in argv
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if trade and not test_env and not real:
        print("REFUSING --trade: would place REAL orders on 正式環境.\n"
              "  use --paper for real-ticks+simulated fills (no real orders, recommended),\n"
              "  or --test-env for 群益模擬 (needs a 測試帳號), or --real to confirm real orders.")
        return 2

    load_dotenv()
    # observe/paper place no real orders -> FUT_ACCOUNT not required; only --trade needs it.
    full_account = os.environ.get("FUT_ACCOUNT", "")
    if trade and not full_account:
        print("REFUSING --trade: FUT_ACCOUNT not set (needed for real orders).")
        return 2
    broker = make_broker({
        "name": "capital_skcom",
        "user_id": os.environ["BROKER_ID"],
        "password": os.environ["BROKER_PWD"],
        "full_account": full_account or "PAPER",
        "cert_id": os.environ.get("CERT_ID", os.environ.get("BROKER_ID", "")),
        "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
        "test_env": test_env,   # observe defaults to 正式 (no orders); trade gated above
    })

    agg = BarAggregator(timeframes=("1m", STRAT_TF))
    if gated:
        strategy = CompositeStrategy(timeframe=STRAT_TF, lookback=LOOKBACK,
                                     breakout_lb=BREAKOUT_LB, flee_lb=FLEE_LB,
                                     enable={"pick", "sell_flee"}, daily_self_gate=True)
    else:
        strategy = CompositeStrategy(timeframe=STRAT_TF, lookback=LOOKBACK,
                                     breakout_lb=BREAKOUT_LB, flee_lb=FLEE_LB)

    stats = {"ticks": 0, "last_price": None, "last_date": None, "prev_ticks": 0}
    paper_sim = {"b": None}   # holds the SimBroker in --paper mode (simulated fills)

    def on_tick(t: Tick):
        stats["ticks"] += 1
        stats["last_price"] = t.price
        if paper_sim["b"] is not None:
            paper_sim["b"].set_mark_time(t.ts)   # stamp simulated fills with market time
        agg.on_tick(t)

    def market_status(now: datetime) -> str:
        """Status from session window + tick flow since the last heartbeat.

        In-session activity is judged by whether new ticks arrived since the
        previous heartbeat (catches holidays without a calendar). Call once per
        heartbeat -- it consumes the new-tick delta.
        """
        sess = clock.session_of(now)
        new_ticks = stats["ticks"] - stats["prev_ticks"]
        stats["prev_ticks"] = stats["ticks"]
        if sess is Session.CLOSED:
            return "非交易時段(時段外)"
        if new_ticks > 0:
            return f"{sess.value}交易中(+{new_ticks})"
        return f"{sess.value}時段內無新tick→可能休市/等待開盤"

    def print_bar(bar):
        print(f"  [{bar.timeframe}] {bar.ts:%m-%d %H:%M} "
              f"O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}")

    notifier = TelegramNotifier()
    if notifier.enabled:
        print("[tg] Telegram notifications ON")
        notifier.send("🤖 微台當沖通知已啟動")

    def make_on_event(tlog: TradeLog):
        def _ev(ev):
            print("EVENT", ev[0], ev[1:])
            if ev[0] == "order":          # ("order", OrderRequest, OrderResult, reason)
                order = ev[1]
                if order.open_close is OpenClose.OPEN:
                    reason = ev[3] if len(ev) > 3 else ""
                    raw = getattr(ev[2], "raw", None)
                    notifier.send(_fmt_entry(order, reason, getattr(raw, "ts", None)))
            elif ev[0] == "close":        # ("close", RoundTrip) -> persist + notify
                tlog.append(ev[1])
                notifier.send(_fmt_exit(ev[1]))
        return _ev

    broker.set_on_tick(on_tick)
    broker.set_on_connection(lambda s: print(f"CONN {s.value}"))

    if paper:
        sim = SimBroker(symbol=SYMBOL)
        paper_sim["b"] = sim
        risk = RiskManager(RiskConfig(max_lots=1, stop_loss_atr_mult=ATR_MULT,
                                      long_stop_atr_mult=LONG_ATR_MULT,
                                      short_stop_atr_mult=SHORT_ATR_MULT,
                                      stop_buffer_atr=STOP_BUFFER_ATR))
        engine = TradingEngine(sim, strategy, risk, PositionStateMachine(),
                               atr_period=ATR_PERIOD,
                               force_close_fn=clock.should_force_close,
                               on_event=make_on_event(TradeLog(
                                   "reports/paper_trades_gated.csv" if gated
                                   else "reports/paper_trades.csv")))
        sim.set_on_trade(engine.on_trade)

        def on_bar(bar):
            print_bar(bar)
            engine.on_bar(bar)
        agg.set_on_bar_close(on_bar)

        def periodic():
            now = datetime.now()
            if stats["last_date"] and now.date() != stats["last_date"]:
                risk.reset_session()
            stats["last_date"] = now.date()
            print(f"[hb] {now:%H:%M:%S} {market_status(now)} ticks={stats['ticks']} "
                  f"last={stats['last_price']} pos={engine.position.state.value} "
                  f"trips={len(engine.round_trips)}")
        variant = "gated p+sf (daily-state)" if gated else "ungated all-6"
        print(f"--- LIVE PAPER (real ticks + simulated fills, NO real orders) "
              f"{SYMBOL} composite [{variant}] lb={LOOKBACK}/{BREAKOUT_LB}/{FLEE_LB} "
              f"atr({ATR_PERIOD}) mult L={LONG_ATR_MULT}/S={SHORT_ATR_MULT} ---")
        broker.serve([SYMBOL], periodic=periodic, period=10.0, with_orders=False)
    elif trade:
        risk = RiskManager(RiskConfig(max_lots=1, stop_loss_atr_mult=ATR_MULT,
                                      long_stop_atr_mult=LONG_ATR_MULT,
                                      short_stop_atr_mult=SHORT_ATR_MULT,
                                      stop_buffer_atr=STOP_BUFFER_ATR))
        engine = TradingEngine(broker, strategy, risk, PositionStateMachine(),
                               atr_period=ATR_PERIOD,
                               force_close_fn=clock.should_force_close,
                               on_event=make_on_event(TradeLog("reports/live_trades.csv")))

        def on_bar(bar):
            print_bar(bar)
            engine.on_bar(bar)
        agg.set_on_bar_close(on_bar)
        broker.set_on_trade(engine.on_trade)

        def periodic():
            now = datetime.now()
            if stats["last_date"] and now.date() != stats["last_date"]:
                risk.reset_session()
            stats["last_date"] = now.date()
            print(f"[hb] {now:%H:%M:%S} {market_status(now)} ticks={stats['ticks']} "
                  f"last={stats['last_price']} pos={engine.position.state.value}")
        env = "測試環境(模擬)" if test_env else "正式環境(REAL)"
        print(f"--- LIVE TRADE [{env}] {SYMBOL} lookback={LOOKBACK} atr_mult={ATR_MULT} max_lots=1 ---")
        broker.serve([SYMBOL], periodic=periodic, period=10.0, with_orders=True)
    else:
        ctx = StrategyContext()

        def on_bar(bar):
            print_bar(bar)
            if bar.timeframe == strategy.timeframe:
                ctx.bars.append(bar)
                sig = strategy.on_bar_close(bar, ctx)
                if sig is not None:
                    print(f"  >> SIGNAL {sig.type.value} @ {sig.price}  ({sig.reason})")
        agg.set_on_bar_close(on_bar)

        def periodic():
            now = datetime.now()
            print(f"[hb] {now:%H:%M:%S} {market_status(now)} ticks={stats['ticks']} "
                  f"last={stats['last_price']}")
        print(f"--- LIVE OBSERVE (no orders) {SYMBOL} ---")
        broker.serve([SYMBOL], periodic=periodic, period=10.0, with_orders=False)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv))
    except KeyboardInterrupt:
        print("\nstopped.")
