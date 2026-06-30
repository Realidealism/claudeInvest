"""On-site SKCOM fill / position field verification (closes the TODO(verify)).

Why this exists
---------------
In --paper mode fills go to SimBroker, so the broker's real OnNewData (fill
report) and OnOpenInterest (positions) callbacks NEVER fire -- their column
layout has never been seen against a real payload. This script places ONE
1-lot order on the REAL account so those callbacks fire. Their raw enumerated
columns are logged at DEBUG (the `... split:` lines are emitted BEFORE any
index-dependent parsing, so they are captured even if the current
_RPT_PRICE / _RPT_QTY guesses are wrong). Paste the two `split:` lines back
and the indices get locked and list_positions() finished.

SAFETY (read before --real)
---------------------------
* 1 lot only; the order is IOC so it never rests in the book.
* STOP the InvestMicroPaper service first -- two SKCOM logins on the same
  account conflict. Restart it when done.
* Default (no --real) = UAT/test env: logs in and reads price only, places
  NO order, risks NO money. Use it first to confirm creds + connectivity.
* --real places a real 1-lot BUY-open then a best-effort 1-lot SELL-cover to
  flatten. Because position parsing is the very thing we are verifying, the
  script CANNOT programmatically confirm you are flat. WATCH YOUR BROKER APP
  and confirm zero net position yourself before walking away.

Usage (during a trading session, paper service stopped):
    # creds via .env in this folder, or set in the shell:
    #   BROKER_ID / BROKER_PWD / FUT_ACCOUNT / CERT_ID(optional)
    python verify_fill.py            # dry: UAT login + price, no order, no money
    python verify_fill.py --real     # REAL account, REAL money, 1 lot round trip
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time

sys.path.insert(0, "src")

from config_env import load_dotenv  # noqa: E402
from broker.factory import make_broker  # noqa: E402
from broker.types import OrderRequest, Side, OpenClose, TimeInForce  # noqa: E402

SYMBOL = "TM0000"          # micro-Taiex near-month (config.yaml: contract.skcom_symbol)
DEFAULT_BUFFER_PTS = 12    # cross the spread by N points so the IOC actually fills

log = logging.getLogger("verify_fill")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", action="store_true",
                    help="正式環境真單真錢 (1 口往返). 預設 UAT: 僅登入+讀價, 不下單")
    ap.add_argument("--buffer", type=int, default=DEFAULT_BUFFER_PTS,
                    help=f"IOC 穿價點數, 越大越確定成交 (預設 {DEFAULT_BUFFER_PTS})")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("reports/verify_fill.log", encoding="utf-8"),
        ],
    )

    load_dotenv()
    try:
        cfg = {
            "name": "capital_skcom",
            "user_id": os.environ["BROKER_ID"],
            "password": os.environ["BROKER_PWD"],
            "full_account": os.environ["FUT_ACCOUNT"],
            "cert_id": os.environ.get("CERT_ID", os.environ.get("BROKER_ID")),
            "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
            "test_env": not args.real,
        }
    except KeyError as exc:
        log.error("缺環境變數 %s -- 先設 BROKER_ID/BROKER_PWD/FUT_ACCOUNT(/CERT_ID) 或放 .env", exc)
        return 2

    last = {"px": None}
    fills = []
    fill_evt = threading.Event()

    def on_tick(t):
        last["px"] = t.price

    def on_trade(tr):
        # NOTE: tr.price/lot come from the UNVERIFIED indices, so they may look
        # wrong here -- that is expected. The raw 'OnNewData split:' log line is
        # the source of truth for locking the indices.
        fills.append(tr)
        log.info("on_trade fired: side=%s price=%s lot=%s (price may be wrong pre-verify)",
                 tr.side.value, tr.price, tr.lot)
        fill_evt.set()

    b = make_broker(cfg)
    b.set_on_tick(on_tick)
    b.set_on_trade(on_trade)
    log.info("connecting (test_env=%s, account=%s)...", cfg["test_env"], cfg["full_account"])
    b.connect()
    b.subscribe(SYMBOL)

    for _ in range(100):
        if last["px"]:
            break
        time.sleep(0.1)
    if not last["px"]:
        log.error("沒收到 tick -- 確認在交易時段且 %s 訂閱成功", SYMBOL)
        b.disconnect()
        return 1
    px = last["px"]
    log.info("last price = %s", px)

    if not args.real:
        log.info("DRY/UAT 模式 OK: 登入+讀價成功, 未下單. 要真實成交驗證請加 --real")
        b.disconnect()
        return 0

    # ---- leg 1: BUY open 1 lot IOC -> fires OnNewData (logs 'OnNewData split:') ----
    buy_px = round(px + args.buffer)
    log.info(">>> REAL: BUY open 1 lot IOC @ %s", buy_px)
    r1 = b.place_order(OrderRequest(SYMBOL, Side.BUY, 1, buy_px,
                                    tif=TimeInForce.IOC, open_close=OpenClose.OPEN))
    log.info("BUY accepted=%s msg=%s", r1.accepted, r1.msg)
    fill_evt.wait(timeout=8.0)

    # ---- capture positions -> fires OnOpenInterest (logs 'OnOpenInterest split:') ----
    log.info(">>> list_positions() to capture OnOpenInterest split")
    try:
        b.list_positions()
    except Exception as exc:  # noqa: BLE001 - diagnostic harness
        log.warning("list_positions raised (expected until parsed): %s", exc)

    # ---- leg 2: best-effort flatten. Explicit COVER is rejected if no position. ----
    fill_evt.clear()
    sell_px = round(px - args.buffer)
    log.info(">>> SELL cover 1 lot IOC @ %s (best-effort flatten)", sell_px)
    r2 = b.place_order(OrderRequest(SYMBOL, Side.SELL, 1, sell_px,
                                    tif=TimeInForce.IOC, open_close=OpenClose.COVER))
    log.info("SELL accepted=%s msg=%s", r2.accepted, r2.msg)
    fill_evt.wait(timeout=8.0)

    time.sleep(2.0)
    b.disconnect()

    print("\n" + "=" * 64)
    print("務必在券商 App 確認【最終無部位】(自動平倉為 best-effort, 不保證)")
    print("把 reports/verify_fill.log 裡這兩種行貼回來:")
    print("  1) OnNewData split: [...]       <- 鎖定成交價/量索引")
    print("  2) OnOpenInterest split: [...]  <- 補完 list_positions")
    print(f"on_trade 觀察到 {len(fills)} 筆: {[(f.side.value, f.price) for f in fills]}")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
