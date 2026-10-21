"""
Backfill stock futures into tw.taifex_futures_daily and seed
tw.stock_futures_contracts (ruling 2026-10-21; the daily scrapers take over
from the run that follows).

Usage (run from repo root), three steps; only the last one writes:

    python -m scripts.backfill.backfill_stock_futures fetch <raw_dir> [first] [last]
        One POST per month (futDataDown, commodity_id=all), raw CSV kept in
        <raw_dir>/YYYYMM.csv; every month gets one line in <raw_dir>/months.csv
        (ok / no_data / failed), failures included. Defaults 2015-12-01 ..
        2026-10-20. Months already saved are not fetched again.
    python -m scripts.backfill.backfill_stock_futures stage <raw_dir> <code_map.csv>
        Parse the saved months with scrapers.taifex_futures._parse, keep the
        three-letter F rows other than TX / MTX / TMF (those are in the table
        already), and write <raw_dir>/rows.parquet and contracts.parquet.
        Read-only on the DB.
    python -m scripts.backfill.backfill_stock_futures write <raw_dir>
        Apply the migrations (init_db), insert rows.parquet (existing rows
        are left as they are) and contracts.parquet.

code_map.csv is the 2026-10-20 research map (tmp/short_sale_research/
futures/): listed_ok rows were on TAIFEX's list that day, matched rows are
delisted codes matched by price, unmatched rows are index / commodity / FX
futures.
"""

import csv
import sys
from datetime import date, timedelta
from pathlib import Path

from scrapers import taifex_futures
from scrapers.taifex_common import download

FIRST, LAST = date(2015, 12, 1), date(2026, 10, 20)
LIST_DAY = date(2026, 10, 20)    # the day code_map's listed codes were on TAIFEX's list
SKIP = taifex_futures.KEEP_CONTRACTS


def _months(first: date, last: date):
    d = date(first.year, first.month, 1)
    while d <= last:
        nxt = date(d.year + d.month // 12, d.month % 12 + 1, 1)
        yield max(d, first), min(nxt - timedelta(days=1), last)
        d = nxt


def fetch(raw: Path, first: date, last: date) -> None:
    raw.mkdir(parents=True, exist_ok=True)
    log = raw / "months.csv"
    done = set()
    if log.exists():
        with log.open(encoding="utf-8") as f:
            done = {r["month"] for r in csv.DictReader(f) if r["status"] != "failed"}
    new = not log.exists()
    with log.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["month", "status", "lines"])
        for a, b in _months(first, last):
            m = a.strftime("%Y%m")
            if m in done:
                continue
            try:
                text = download(taifex_futures.URL, {
                    "down_type": "1", "commodity_id": "all", "commodity_id2": "",
                    "queryStartDate": a.strftime("%Y/%m/%d"), "queryEndDate": b.strftime("%Y/%m/%d")})
            except Exception as e:      # recorded, not fatal: the month is fetched again next time
                print(f"{m} failed: {e}", flush=True)
                w.writerow([m, "failed", 0])
                continue
            if text is None:
                w.writerow([m, "no_data", 0])
            else:
                (raw / f"{m}.csv").write_text(text, encoding="utf-8")
                w.writerow([m, "ok", text.count("\n")])
            f.flush()
            print(f"{m} {'ok' if text else 'no_data'}", flush=True)


def stage(raw: Path, code_map: Path) -> None:
    import pandas as pd
    rows = []
    for p in sorted(raw.glob("[0-9]" * 6 + ".csv")):
        data, api, err = taifex_futures._parse(p.read_text(encoding="utf-8"))
        if err:
            print(f"{p.stem}: {err} parse errors of {api}")
        rows += [r for r in data if r["contract"] not in SKIP]
    df = pd.DataFrame(rows)
    df.to_parquet(raw / "rows.parquet", index=False)

    cm = pd.read_csv(code_map, dtype=str, keep_default_na=False)
    lot = pd.to_numeric(cm["lot"], errors="coerce")
    out = pd.DataFrame({
        "contract": cm["contract"],
        "kind": cm["status"].map({"listed_ok": "stock", "matched": "stock", "unmatched": "other"}),
        "stock_id": cm["stock_id"].where(cm["status"] != "unmatched", None).replace("", None),
        "shares_per_lot": lot.astype("Int64"),
        "standard": cm["standard"].eq("True") & cm["status"].ne("unmatched"),
        "source": cm["status"].map({"listed_ok": "stockLists"}).fillna("price_match"),
        "last_listed": [LIST_DAY if s == "listed_ok" else None for s in cm["status"]],
    })
    assert out["kind"].notna().all(), cm.loc[out["kind"].isna(), "status"].unique()
    out.to_parquet(raw / "contracts.parquet", index=False)
    print(f"rows {len(df)}, contracts {len(out)}")


_ROW_SQL = """
    INSERT INTO tw.taifex_futures_daily (
        trade_date, contract, contract_month, session,
        open_price, high_price, low_price, close_price, change, change_pct,
        volume, settlement, open_interest, best_bid, best_ask,
        hist_high, hist_low, halted, spread_volume
    ) VALUES (
        %(trade_date)s, %(contract)s, %(contract_month)s, %(session)s,
        %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
        %(change)s, %(change_pct)s, %(volume)s, %(settlement)s,
        %(open_interest)s, %(best_bid)s, %(best_ask)s,
        %(hist_high)s, %(hist_low)s, %(halted)s, %(spread_volume)s
    )
    ON CONFLICT (trade_date, contract, contract_month, session) DO NOTHING
"""
_CONTRACT_SQL = """
    INSERT INTO tw.stock_futures_contracts
        (contract, kind, stock_id, shares_per_lot, standard, source, last_listed)
    VALUES (%(contract)s, %(kind)s, %(stock_id)s, %(shares_per_lot)s, %(standard)s,
            %(source)s, %(last_listed)s)
    ON CONFLICT (contract) DO NOTHING
"""


def _records(df):
    """Rows as plain Python values (psycopg2 does not adapt numpy scalars)."""
    import pandas as pd
    return [{k: None if pd.isna(v) else v.item() if hasattr(v, "item") else v for k, v in r.items()}
            for r in df.astype(object).to_dict("records")]


def write(raw: Path) -> None:
    import pandas as pd
    from psycopg2.extras import execute_batch
    from db.connection import get_cursor, init_db
    init_db()
    rows = pd.read_parquet(raw / "rows.parquet")
    contracts = pd.read_parquet(raw / "contracts.parquet")
    with get_cursor() as cur:
        execute_batch(cur, _CONTRACT_SQL, _records(contracts), page_size=500)
        print(f"contracts: {len(contracts)} offered", flush=True)
        recs = _records(rows)
        for i in range(0, len(recs), 50000):
            execute_batch(cur, _ROW_SQL, recs[i:i + 50000], page_size=1000)
            print(f"rows: {min(i + 50000, len(recs))}/{len(recs)}", flush=True)


if __name__ == "__main__":
    step, raw = sys.argv[1], Path(sys.argv[2])
    if step == "fetch":
        first = date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 else FIRST
        last = date.fromisoformat(sys.argv[4]) if len(sys.argv) > 4 else LAST
        fetch(raw, first, last)
    elif step == "stage":
        stage(raw, Path(sys.argv[3]))
    elif step == "write":
        write(raw)
    else:
        sys.exit(f"unknown step {step}")
