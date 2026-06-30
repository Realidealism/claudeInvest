"""Probe candidate symbols against the Capital quote server.

RUN ON THE SKCOM MACHINE. Reads .env (same as fetch_kbars). Prints, for each
candidate code, the GetStockByNoLONG return code + product name, so you can find
the correct micro-Taiex near-month code.

    python tools\\check_symbol.py
    python tools\\check_symbol.py TMF00 TMFR1 MTX00   # custom list
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from broker.factory import make_broker        # noqa: E402
from config_env import load_dotenv            # noqa: E402

# default candidates to probe (micro / mini / full Taiex near-month variants)
_DEFAULT = ["TMF00", "TMFR1", "TMF", "MTX00", "MXF00", "TXF00", "TX00", "TXFR1"]


def main(argv) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv()
    candidates = argv[1:] or _DEFAULT

    broker = make_broker({
        "name": "capital_skcom",
        "user_id": os.environ["BROKER_ID"],
        "password": os.environ["BROKER_PWD"],
        "full_account": os.environ["FUT_ACCOUNT"],
        "cert_id": os.environ.get("CERT_ID", os.environ["BROKER_ID"]),
        "skcom_dll_path": os.environ.get("SKCOM_DLL", r"C:\SKCOM\x64\SKCOM.dll"),
        "test_env": os.environ.get("SKCOM_TEST_ENV", "0") == "1",
    })
    broker.connect(quote_only=True, background_pump=False)

    print("\n--- symbol probe ---")
    for sym in candidates:
        code, name, dec = broker.stock_info(sym)
        ok = "OK " if code == 0 else "no "
        print(f"  {ok} {sym:8} nCode={code} name={name} decimal={dec}")

    print("\n--- futures product list (market=2), entries containing '微' ---")
    import re
    data = "|".join(broker.list_commodities(2))
    print(f"(raw length={len(data)})")
    hits = re.findall(r".{0,40}微.{0,40}", data)
    if hits:
        for h in hits:
            print("  ...", h, "...")
    else:
        print("  no '微' found; raw sample:")
        print(" ", data[:1500])
    broker.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
