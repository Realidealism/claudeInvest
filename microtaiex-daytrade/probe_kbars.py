"""One-shot probe: does get_kbars return TODAY's intraday 5m bars during live?

RUN ON THE SKCOM MACHINE. Read-only, quote-only, NO orders, zero risk.
Connects, fetches yesterday..today 5m bars, prints count + time span + the last
10 bars, and compares the last bar's time to wall-clock now. If the last bar is
within a few minutes of now, preload-on-restart is viable.

    python probe_kbars.py            # TM0000, yesterday..today
    python probe_kbars.py MTX00      # another symbol
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from broker.factory import make_broker   # noqa: E402
from config_env import load_dotenv       # noqa: E402


def main(argv) -> int:
    symbol = argv[1] if len(argv) > 1 else "TM0000"
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv()

    now = datetime.now()
    # end = current trading day. The night session (15:00→next 05:00) is archived
    # under the NEXT calendar day's trading day, so in the evening (>=14:00) the
    # current trading day is tomorrow; after midnight it is already today.
    cur_td = now.date() + timedelta(days=1) if now.hour >= 14 else now.date()
    end_day = cur_td.strftime("%Y%m%d")
    start_day = (now - timedelta(days=3)).strftime("%Y%m%d")

    broker = make_broker({
        "name": "capital_skcom",
        "user_id": os.environ["BROKER_ID"],
        "password": os.environ["BROKER_PWD"],
        "full_account": os.environ.get("FUT_ACCOUNT", "PAPER"),
        "cert_id": os.environ.get("CERT_ID", os.environ["BROKER_ID"]),
        "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
        "test_env": os.environ.get("SKCOM_TEST_ENV", "0") == "1",
    })
    broker.connect(quote_only=True, background_pump=False)
    bars = broker.get_kbars(symbol, start_day, end_day, minute=5, session=0, kline_type=0)
    broker.disconnect()

    print(f"\n=== probe get_kbars({symbol}, {start_day}..{end_day}, minute=5) ===")
    print(f"wall-clock now : {now:%Y-%m-%d %H:%M:%S}")
    print(f"bars returned  : {len(bars)}")
    if not bars:
        print("!! no bars returned — check symbol / market hours / history depth")
        return 1
    print(f"first bar ts   : {bars[0].ts:%Y-%m-%d %H:%M}")
    print(f"last  bar ts   : {bars[-1].ts:%Y-%m-%d %H:%M}")
    lag_min = (now - bars[-1].ts).total_seconds() / 60.0
    print(f"last bar lag   : {lag_min:.0f} min behind now")
    print("\nlast 10 bars (ts  O/H/L/C  V):")
    for b in bars[-10:]:
        print(f"  {b.ts:%m-%d %H:%M}  {b.open:.0f}/{b.high:.0f}/{b.low:.0f}/{b.close:.0f}  V={b.volume}")
    print("\n>> preload viable if 'last bar lag' is small (~<10 min) during live hours.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
