"""Daily hermit-stock screener snapshot + diff.

Called from the parent project's daily_update.py at the end of the daily
scraper pipeline. Runs the screener (gate F6+F7+F8, floor=3, top 50),
saves the result to tw.hermit_screen_snapshot, and prints a day-over-day
diff (NEW entrants, EXITs, rank shifts).

Cost is ~20 seconds (screener Stage-1 + Stage-2 valuation for ~200
candidates) regardless of whether new financial data landed. Storage is
~10 KB per day in DB. Cheap enough to run unconditionally each weekday.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from psycopg2.extras import RealDictCursor, execute_batch

from .data.adapters.db_adapter import _connect
from .reports.screener import ScreenRow, screen_from_db


GATE_RULES = frozenset({"F6", "F7", "F8"})
FLOOR = 3
TOP_N = 50


@dataclass(frozen=True)
class DiffRow:
    stock_id: str
    name: str
    today_rank: int | None
    prev_rank: int | None
    today_score: int | None
    prev_score: int | None
    decision: str | None

    @property
    def status(self) -> str:
        if self.prev_rank is None:
            return "NEW"
        if self.today_rank is None:
            return "EXIT"
        if self.today_rank < self.prev_rank:
            return f"UP {self.prev_rank - self.today_rank}"
        if self.today_rank > self.prev_rank:
            return f"DN {self.today_rank - self.prev_rank}"
        return "FLAT"


def _previous_snapshot_date(snapshot_date: date) -> date | None:
    sql = """
        SELECT MAX(snapshot_date) AS d
        FROM tw.hermit_screen_snapshot
        WHERE snapshot_date < %s
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (snapshot_date,))
        row = cur.fetchone()
    return row["d"] if row and row["d"] else None


def _load_snapshot(d: date) -> dict[str, dict]:
    """Return {stock_id: {rank, score, decision, ...}} for a given date."""
    sql = """
        SELECT stock_id, rank, score, val_decision
        FROM tw.hermit_screen_snapshot
        WHERE snapshot_date = %s
    """
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (d,))
        rows = cur.fetchall()
    return {r["stock_id"]: dict(r) for r in rows}


def _save_snapshot(snapshot_date: date, rows: list[ScreenRow], prev: dict[str, dict]) -> int:
    """Bulk insert today's top N. Idempotent via ON CONFLICT DO UPDATE."""
    insert_rows = []
    for i, r in enumerate(rows[:TOP_N], start=1):
        by_code = {rr.code: rr for rr in r.rule_results}

        def passed(code: str) -> bool | None:
            rr = by_code.get(code)
            return rr.passed if rr else None

        v = r.valuation
        prev_row = prev.get(r.ticker)
        prev_rank = prev_row["rank"] if prev_row else None
        rank_delta = (prev_rank - i) if prev_rank is not None else None
        is_new = prev_rank is None

        insert_rows.append((
            snapshot_date,
            i,
            r.ticker,
            r.scoreboard.score,
            r.scoreboard.grade,
            passed("F1"), passed("F2"), passed("F3"), passed("F4"),
            passed("F5"), passed("F6"), passed("F7"), passed("F8"),
            v.method if v else None,
            round(v.current_multiple, 2) if v and v.current_multiple else None,
            v.band_position if v else None,
            round(v.upside_mean * 100, 2) if v and v.upside_mean is not None else None,
            v.decision if v else None,
            is_new,
            prev_rank,
            rank_delta,
        ))

    sql = """
        INSERT INTO tw.hermit_screen_snapshot
        (snapshot_date, rank, stock_id, score, grade,
         f1_pass, f2_pass, f3_pass, f4_pass, f5_pass, f6_pass, f7_pass, f8_pass,
         val_method, val_multiple, val_band, val_upside_pct, val_decision,
         is_new, prev_rank, rank_delta)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (snapshot_date, stock_id) DO UPDATE SET
            rank = EXCLUDED.rank,
            score = EXCLUDED.score,
            grade = EXCLUDED.grade,
            f1_pass = EXCLUDED.f1_pass, f2_pass = EXCLUDED.f2_pass,
            f3_pass = EXCLUDED.f3_pass, f4_pass = EXCLUDED.f4_pass,
            f5_pass = EXCLUDED.f5_pass, f6_pass = EXCLUDED.f6_pass,
            f7_pass = EXCLUDED.f7_pass, f8_pass = EXCLUDED.f8_pass,
            val_method = EXCLUDED.val_method,
            val_multiple = EXCLUDED.val_multiple,
            val_band = EXCLUDED.val_band,
            val_upside_pct = EXCLUDED.val_upside_pct,
            val_decision = EXCLUDED.val_decision,
            is_new = EXCLUDED.is_new,
            prev_rank = EXCLUDED.prev_rank,
            rank_delta = EXCLUDED.rank_delta
    """
    with _connect() as conn, conn.cursor() as cur:
        execute_batch(cur, sql, insert_rows)
        conn.commit()
    return len(insert_rows)


def _print_diff(snapshot_date: date, rows: list[ScreenRow], prev: dict[str, dict]) -> None:
    name_by = {r.ticker: r.name for r in rows}
    today_set = {r.ticker for r in rows[:TOP_N]}
    prev_set = set(prev.keys())

    new_ticks = [r for r in rows[:TOP_N] if r.ticker not in prev_set]
    exit_ticks = [t for t in prev_set if t not in today_set]

    rank_today = {r.ticker: i + 1 for i, r in enumerate(rows[:TOP_N])}
    big_movers = []
    for t in today_set & prev_set:
        delta = prev[t]["rank"] - rank_today[t]
        if abs(delta) >= 5:
            big_movers.append((t, prev[t]["rank"], rank_today[t], delta))

    print(f"  Top-{TOP_N} snapshot: {snapshot_date}")
    if not prev:
        print(f"  (first snapshot — no diff)")
        return
    print(f"  vs prev snapshot: {len(new_ticks)} NEW, {len(exit_ticks)} EXIT, "
          f"{len(big_movers)} big-movers")

    if new_ticks:
        print("  NEW entrants:")
        for r in new_ticks[:10]:
            print(f"    + {r.ticker} {name_by.get(r.ticker, '')} "
                  f"(rank {rank_today[r.ticker]}, score {r.scoreboard.score}/8, "
                  f"{r.valuation.decision if r.valuation else '-'})")
        if len(new_ticks) > 10:
            print(f"    ... +{len(new_ticks) - 10} more")

    if exit_ticks:
        print("  EXITs:")
        for t in list(exit_ticks)[:10]:
            print(f"    - {t} (was rank {prev[t]['rank']}, score {prev[t]['score']}/8)")
        if len(exit_ticks) > 10:
            print(f"    ... +{len(exit_ticks) - 10} more")

    if big_movers:
        print("  Big movers (≥5 rank change):")
        for t, old, new, delta in sorted(big_movers, key=lambda x: -abs(x[3]))[:10]:
            arrow = "↑" if delta > 0 else "↓"
            print(f"    {arrow} {t} {name_by.get(t, '')}: {old} → {new} ({delta:+d})")


def run(snapshot_date: date) -> dict[str, int]:
    """Main entry. Returns counts {top_n, new_count, exit_count, big_mover_count}."""
    print(f"  hermit_stock daily check @ {snapshot_date} ...")
    rows = screen_from_db(
        as_of_label=snapshot_date.isoformat(),
        with_valuation=True,
        min_valuation_score=FLOOR,
    )
    # Apply gate filter (must pass F6+F7+F8) — same as production strategy
    def _passes_gates(r: ScreenRow) -> bool:
        by_code = {rr.code: rr for rr in r.rule_results}
        return all(by_code.get(g) and by_code[g].passed is True for g in GATE_RULES)

    rows = [r for r in rows if _passes_gates(r)]
    rows.sort(key=lambda r: (-r.scoreboard.score, r.ticker))

    prev_date = _previous_snapshot_date(snapshot_date)
    prev_snap = _load_snapshot(prev_date) if prev_date else {}

    n = _save_snapshot(snapshot_date, rows, prev_snap)
    _print_diff(snapshot_date, rows, prev_snap)

    today_set = {r.ticker for r in rows[:TOP_N]}
    return {
        "top_n": n,
        "new_count": len(today_set - set(prev_snap.keys())),
        "exit_count": len(set(prev_snap.keys()) - today_set),
    }


if __name__ == "__main__":
    import sys

    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    print(run(d))
