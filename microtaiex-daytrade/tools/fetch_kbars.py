"""Fetch historical micro-Taiex K bars from Capital SKCOM and save to CSV.

RUN ON THE SKCOM MACHINE (64-bit Windows with the API component registered).
Needs env: BROKER_ID / BROKER_PWD / FUT_ACCOUNT / CERT_ID.

Example:
    set BROKER_ID=...
    set BROKER_PWD=...
    set FUT_ACCOUNT=F020000-1234567
    python tools\\fetch_kbars.py TMF00 20240101 20240630 5 reports\\tmf_5m.csv

Output CSV columns: ts,open,high,low,close,volume  (loadable by data.csv_loader).
"""
from __future__ import annotations

import csv
import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from broker.factory import make_broker   # noqa: E402
from config_env import load_dotenv       # noqa: E402


def main(argv) -> int:
    if len(argv) < 6:
        print("usage: fetch_kbars.py SYMBOL START(YYYYMMDD) END(YYYYMMDD) MINUTE OUT.csv")
        return 2
    symbol, start, end, minute, out = argv[1], argv[2], argv[3], int(argv[4]), argv[5]

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    loaded = load_dotenv()
    if loaded:
        print(f"loaded env from {loaded}")

    broker = make_broker({
        "name": "capital_skcom",
        "user_id": os.environ["BROKER_ID"],
        "password": os.environ["BROKER_PWD"],
        "full_account": os.environ["FUT_ACCOUNT"],
        "cert_id": os.environ.get("CERT_ID", os.environ["BROKER_ID"]),
        "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
        "test_env": os.environ.get("SKCOM_TEST_ENV", "0") == "1",
    })
    # quote_only: history needs the quote server only.
    # background_pump=False: pump COM messages inline on this thread so STA
    # events (OnConnection / OnNotifyKLineData) are actually delivered.
    broker.connect(quote_only=True, background_pump=False)
    bars = broker.get_kbars(symbol, start, end, minute=minute, session=0)
    broker.disconnect()

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open", "high", "low", "close", "volume"])
        for b in bars:
            w.writerow([b.ts.isoformat(), b.open, b.high, b.low, b.close, b.volume])

    print(f"wrote {len(bars)} bars -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
