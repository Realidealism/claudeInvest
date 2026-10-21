"""
Backfill the exchanges' ex-rights results into tw.dividends from 2026-04-30,
the day after the FinMind rows stop (ruling 2026-10-21; the daily scraper
scrapers.ex_rights_results takes over).

Usage (run from repo root); only the last step writes:

    python -m scripts.backfill.backfill_ex_rights fetch <dir> [first] [last]
        One collect() per month (TWSE TWT49U + details, TPEx exDailyQ); every
        answer kept in <dir>/raw/, the events in <dir>/events/YYYYMM.json, and
        one line per month in <dir>/months.csv with both legs' states.
        Months already ok are not fetched again. Defaults 2026-04-30 .. 2026-10-20.
    python -m scripts.backfill.backfill_ex_rights stage <dir>
        <dir>/rows.parquet: what write would upsert.
    python -m scripts.backfill.backfill_ex_rights write <dir>
        Apply the migrations (init_db), upsert rows.parquet and drop the
        FinMind rows of the same stock and ex-date.
"""

import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path

from scrapers import ex_rights_results as xr

FIRST, LAST = date(2026, 4, 30), date(2026, 10, 20)


def _months(first: date, last: date):
    d = date(first.year, first.month, 1)
    while d <= last:
        nxt = date(d.year + d.month // 12, d.month % 12 + 1, 1)
        yield max(d, first), min(nxt - timedelta(days=1), last)
        d = nxt


def fetch(out: Path, first: date, last: date) -> None:
    (out / "raw").mkdir(parents=True, exist_ok=True)
    (out / "events").mkdir(exist_ok=True)
    log = out / "months.csv"
    done = set()
    if log.exists():
        with log.open(encoding="utf-8") as f:
            done = {r["month"] for r in csv.DictReader(f) if r["twse"] == "ok" and r["tpex"] in ("ok", "empty")}
    new = not log.exists()
    with log.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["month", "twse", "tpex", "events"])
        for a, b in _months(first, last):
            m = a.strftime("%Y%m")
            if m in done:
                continue

            def sink(name, data):
                (out / "raw" / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            try:
                events, legs = xr.collect(a, b, raw_sink=sink)
            except Exception as e:      # recorded, not fatal: the month is fetched again next time
                w.writerow([m, f"error {e}", "", 0])
                f.flush()
                continue
            (out / "events" / f"{m}.json").write_text(json.dumps(events, ensure_ascii=False, default=str),
                                                      encoding="utf-8")
            w.writerow([m, legs["上市除權息結果"], legs["上櫃除權息結果"], len(events)])
            f.flush()
            print(m, legs, len(events), flush=True)


def _events(out: Path) -> list[dict]:
    events = []
    for p in sorted((out / "events").glob("*.json")):
        for e in json.loads(p.read_text(encoding="utf-8")):
            e["ex_date"] = date.fromisoformat(e["ex_date"])
            events.append(e)
    return events


def stage(out: Path) -> None:
    import pandas as pd
    rows = xr.to_rows(_events(out))
    pd.DataFrame(rows).to_parquet(out / "rows.parquet", index=False)
    print(f"rows {len(rows)}")


def write(out: Path) -> None:
    import pandas as pd
    from db.connection import init_db
    init_db()
    rows = pd.read_parquet(out / "rows.parquet").to_dict("records")
    for r in rows:
        r["cash"], r["stock"] = float(r["cash"]), float(r["stock"])
    unlisted = xr.write(rows)
    print(f"upserted {sum(r['stock_id'] not in unlisted for r in rows)} of {len(rows)}; "
          f"not in tw.stocks ({len(unlisted)} codes): {unlisted}")


if __name__ == "__main__":
    step, out = sys.argv[1], Path(sys.argv[2])
    if step == "fetch":
        fetch(out, date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 else FIRST,
              date.fromisoformat(sys.argv[4]) if len(sys.argv) > 4 else LAST)
    elif step == "stage":
        stage(out)
    elif step == "write":
        write(out)
    else:
        sys.exit(f"unknown step {step}")
