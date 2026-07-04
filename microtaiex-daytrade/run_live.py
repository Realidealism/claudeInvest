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
from strategy.strategies.masha import MashaStrategy  # noqa: E402
from strategy.timing import in_tradeable_window     # noqa: E402
from strategy.trendline import channel              # noqa: E402
from notify import TelegramNotifier, STATUS_KEYBOARD  # noqa: E402

_POINT_VALUE = 10   # micro-Taiex NT$ per index point


def _side_zh(side) -> str:
    return "做多" if side is Side.BUY else "做空"


def _fmt_entry(order, reason, ts, defense=None) -> str:
    emoji = "🔴" if order.side is Side.BUY else "🟢"   # 台股慣例：紅漲(多)/綠跌(空)
    sig = f"　訊號 {reason}" if reason else ""
    when = f"　{ts:%m-%d %H:%M}" if ts else ""
    dfn = ""
    if defense is not None:
        risk = (defense - order.price) if order.side is Side.BUY else (order.price - defense)
        dfn = f"\n防守 {defense:.0f}（{risk:+.0f} 點）"
    return (f"{emoji} 進場 {_side_zh(order.side)} {order.symbol}\n"
            f"價格 {order.price:.0f}{sig}{when}{dfn}")


def _fmt_defense(side, defense, entry_price, symbol) -> str:
    pts = (defense - entry_price) if side is Side.BUY else (entry_price - defense)
    tag = "🔼 防守拉高" if side is Side.BUY else "🔽 防守下移"
    return f"{tag} {_side_zh(side)} {symbol}\n{defense:.0f}（{pts:+.0f} 點）"


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
# Day-trade (daily force-close) sweep: with 3 long signals (pick/sell_flee/buy)
# the sweet spot is a TIGHT long stop (mult 3) + fast re-entry — PF 1.30 stable
# across both halves, more net + smaller tail than the old wide-stop 6.
LONG_ATR_MULT = 3.0
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
    # --masha: run the 麻紗 軌道 rail strategy as a parallel shadow engine in the
    # same --paper process (one COM login, own SimBroker/risk/log/TG-prefix).
    masha = "--masha" in argv
    # --warmup: before serve(), seed engines with recent closed 5m bars (fetch via
    # a throwaway quote connection) so ATR/軌道 are armed immediately after restart.
    warmup = "--warmup" in argv
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
    broker_cfg = {
        "name": "capital_skcom",
        "user_id": os.environ["BROKER_ID"],
        "password": os.environ["BROKER_PWD"],
        "full_account": full_account or "PAPER",
        "cert_id": os.environ.get("CERT_ID", os.environ.get("BROKER_ID", "")),
        "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
        "test_env": test_env,   # observe defaults to 正式 (no orders); trade gated above
    }
    broker = make_broker(broker_cfg)

    agg = BarAggregator(timeframes=("1m", STRAT_TF))
    if gated:
        # gate by the daily signal factory's TX unified state (the 大台 signal)
        tx_status = str(_ROOT.parent / "frontend" / "public" / "data" / "futures_tx.json")
        strategy = CompositeStrategy(timeframe=STRAT_TF, lookback=LOOKBACK,
                                     breakout_lb=BREAKOUT_LB, flee_lb=FLEE_LB,
                                     enable={"pick", "sell_flee", "buy"}, tx_status_path=tx_status)
    else:
        strategy = CompositeStrategy(timeframe=STRAT_TF, lookback=LOOKBACK,
                                     breakout_lb=BREAKOUT_LB, flee_lb=FLEE_LB)

    stats = {"ticks": 0, "last_price": None, "last_date": None, "prev_ticks": 0}
    paper_sims = []   # SimBrokers in --paper mode (one per shadow engine)

    def on_tick(t: Tick):
        stats["ticks"] += 1
        stats["last_price"] = t.price
        for s in paper_sims:
            s.set_mark_time(t.ts)   # stamp simulated fills with market time
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

    def make_on_event(tlog: TradeLog, tag: str = ""):
        pre = f"【{tag}】\n" if tag else ""       # 麻紗 shadow prefixes its messages
        def _ev(ev):
            print("EVENT", tag or "composite", ev[0], ev[1:])
            if ev[0] == "order":          # ("order", OrderRequest, OrderResult, reason)
                order = ev[1]
                if order.open_close is OpenClose.OPEN:
                    reason = ev[3] if len(ev) > 3 else ""
                    defense = ev[4] if len(ev) > 4 else None
                    raw = getattr(ev[2], "raw", None)
                    notifier.send(pre + _fmt_entry(order, reason, getattr(raw, "ts", None), defense))
            elif ev[0] == "defense":      # ("defense", side, price, entry, symbol)
                notifier.send(pre + _fmt_defense(ev[1], ev[2], ev[3], ev[4]))
            elif ev[0] == "close":        # ("close", RoundTrip) -> persist + notify
                tlog.append(ev[1])
                notifier.send(pre + _fmt_exit(ev[1]))
        return _ev

    broker.set_on_tick(on_tick)
    broker.set_on_connection(lambda s: print(f"CONN {s.value}"))

    if paper:
        engines = []   # (engine, risk, label) — composite + optional 麻紗 shadow

        def add_paper_engine(strat, risk_cfg, tlog_path, tag, label):
            sim = SimBroker(symbol=SYMBOL)
            paper_sims.append(sim)
            risk = RiskManager(risk_cfg)
            eng = TradingEngine(sim, strat, risk, PositionStateMachine(),
                                atr_period=ATR_PERIOD,
                                force_close_fn=clock.should_force_close,
                                on_event=make_on_event(TradeLog(tlog_path), tag))
            sim.set_on_trade(eng.on_trade)
            engines.append((eng, risk, label))
            return eng

        add_paper_engine(
            strategy,
            RiskConfig(max_lots=1, stop_loss_atr_mult=ATR_MULT,
                       long_stop_atr_mult=LONG_ATR_MULT, short_stop_atr_mult=SHORT_ATR_MULT,
                       stop_buffer_atr=STOP_BUFFER_ATR,
                       # unified 初始防守: 麻紗 dynamic per-trade stop as the hard
                       # initial cap (0.5xATR clamp 12..40, warmup fallback 20);
                       # the Chandelier trail stays as the trailing exit.
                       max_loss_points_per_trade=20.0, masha_loss_atr_mult=0.5),
            "reports/paper_trades_gated.csv" if gated else "reports/paper_trades.csv",
            "", "composite")
        if masha:
            # 麻紗 軌道 rail primary (backtest-validated config: track lb40/mt2/rt40
            # engulf+doji, ungated). 出村 caps + entry-bar/20pt stop + session flat.
            add_paper_engine(
                MashaStrategy(timeframe=STRAT_TF, trend_gate=None),
                RiskConfig(max_lots=1, stop_mode="masha",
                           max_loss_points_per_trade=20.0, max_daily_loss_points=60.0,
                           daily_profit_target_points=50.0, masha_loss_atr_mult=0.5),
                "reports/paper_trades_masha.csv", "麻紗", "麻紗")

        def on_bar(bar):
            print_bar(bar)
            for eng, _, _ in engines:
                eng.on_bar(bar)
        agg.set_on_bar_close(on_bar)

        def build_status() -> str:
            """On-demand snapshot for the 🔍 狀態 Telegram button."""
            now = datetime.now()
            lines = [f"📊 狀態 {now:%m-%d %H:%M:%S}　{market_status(now)}",
                     f"最新價 {stats['last_price']}　ticks={stats['ticks']}"]
            for eng, risk, lbl in engines:
                pos = eng.position
                seg = f"〔{lbl}〕{pos.state.value}"
                if pos.is_holding():
                    d = "做多" if pos.side is Side.BUY else "做空"
                    seg += f"　{d} @ {pos.entry_price:.0f}"
                seg += f"　今日 {len(eng.round_trips)} 筆"
                if risk.halted:
                    seg += "（停手）"
                lines.append(seg)
                if lbl == "麻紗":
                    strat, bars = eng._strategy, eng._ctx.bars
                    inw = "開" if in_tradeable_window(now) else "關"
                    lines.append(f"　進場窗 {inw}　bars={len(bars)}")
                    if len(bars) >= 5:
                        dirn, lo, up = channel(bars, strat.track_k, strat.track_lookback,
                                               strat.track_tol, strat.track_min_touches)
                        c = bars[-1].close
                        if dirn == "up" and lo is not None:
                            lines.append(f"　軌道 上升｜下軌 {lo:.0f}｜現價 {c:.0f}（距 {c-lo:+.0f}）")
                        elif dirn == "down" and up is not None:
                            lines.append(f"　軌道 下降｜上軌 {up:.0f}｜現價 {c:.0f}（距 {c-up:+.0f}）")
                        else:
                            lines.append("　軌道 無趨勢（range）")
            return "\n".join(lines)

        def periodic():
            now = datetime.now()
            if stats["last_date"] and now.date() != stats["last_date"]:
                for _, risk, _ in engines:
                    risk.reset_session()
            stats["last_date"] = now.date()
            pos_str = "  ".join(f"{lbl}:{eng.position.state.value}/{len(eng.round_trips)}"
                                for eng, _, lbl in engines)
            print(f"[hb] {now:%H:%M:%S} {market_status(now)} ticks={stats['ticks']} "
                  f"last={stats['last_price']}  {pos_str}")
            for cmd in notifier.poll_commands():          # 🔍 狀態 button / /status
                if "狀態" in cmd or cmd.lstrip("/").lower().startswith("status"):
                    notifier.send(build_status())
        # warmup (single login): serve() calls this AFTER the quote server is ready
        # (before tick subscription), so we fetch recent closed 5m bars on THE SAME
        # login — no second login — and seed every engine, arming ATR(21)/軌道(40)
        # immediately after a restart. serve() swallows any failure of this hook.
        def _do_warmup():
            from datetime import timedelta
            _now = datetime.now()
            # end = current trading day. The night session (15:00→next 05:00) is
            # archived under the NEXT calendar day's trading day, so in the evening
            # (>=14:00) the current trading day is tomorrow; after midnight it is
            # already today. A blind +1 would overshoot past midnight.
            _cur_td = _now.date() + timedelta(days=1) if _now.hour >= 14 else _now.date()
            _start = (_now - timedelta(days=3)).strftime("%Y%m%d")
            _end = _cur_td.strftime("%Y%m%d")
            warm = broker.get_kbars(SYMBOL, _start, _end, minute=5)
            warm = warm[-120:] if warm else []
            for eng, _, _ in engines:
                eng.preload(warm)
            print(f"[preload] {_start}..{_end} → seeded {len(warm)} bars"
                  + (f" up to {warm[-1].ts:%m-%d %H:%M}" if warm else ""))

        variant = "gated p+sf (daily-state)" if gated else "ungated all-6"
        shadow = " + 麻紗軌道 shadow" if masha else ""
        print(f"--- LIVE PAPER (real ticks + simulated fills, NO real orders) "
              f"{SYMBOL} composite [{variant}]{shadow} lb={LOOKBACK}/{BREAKOUT_LB}/{FLEE_LB} "
              f"atr({ATR_PERIOD}) mult L={LONG_ATR_MULT}/S={SHORT_ATR_MULT} ---")
        if notifier.enabled:
            notifier.send("🤖 微台當沖 paper 已啟動，按下方「🔍 狀態」查詢即時狀態",
                          reply_markup=STATUS_KEYBOARD)
            notifier.poll_commands()   # drain pre-restart backlog + prime the offset
        broker.serve([SYMBOL], periodic=periodic, period=10.0, with_orders=False,
                     on_ready=(_do_warmup if warmup else None))
    elif trade:
        risk = RiskManager(RiskConfig(max_lots=1, stop_loss_atr_mult=ATR_MULT,
                                      long_stop_atr_mult=LONG_ATR_MULT,
                                      short_stop_atr_mult=SHORT_ATR_MULT,
                                      stop_buffer_atr=STOP_BUFFER_ATR,
                                      # unified 初始防守: 麻紗 dynamic per-trade stop
                                      # (0.5xATR clamp 12..40, warmup fallback 20).
                                      max_loss_points_per_trade=20.0,
                                      masha_loss_atr_mult=0.5))
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
