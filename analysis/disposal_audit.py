"""Disposal-prediction audit.

For each ``audit_date`` (= the day disposal would START):
  1. Compute the bot's prediction at EOD of (audit_date - 1 trading day).
  2. Pull TWSE's actual disposal announcements with ``period_start = audit_date``.
  3. Insert one row per (audit_date, stock_id) into
     ``tw.disposal_prediction_audit``.
  4. Telegram-push a summary of discrepancies (false positives + false
     negatives) for that audit_date.

Universe: any stock with at least one attention announcement in the 30
trading days ending at audit_date - 1, plus any stock that actually
entered disposal on audit_date (so we never miss an unexpected trigger).

Usage:
    python -m analysis.disposal_audit                # audit today
    python -m analysis.disposal_audit 2026-06-10     # audit a specific date
    python -m analysis.disposal_audit --backfill 14  # last 14 trading days
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta

from db.connection import get_cursor
from telegram_bot.handlers.score import predict_disposal_trigger
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

_LOOKBACK_TD = 30  # trading-day window for 款 bucket counting


def _trading_days_back(cur, end: date, n: int) -> list[date]:
    cur.execute(
        """
        SELECT trade_date FROM tw.index_prices
        WHERE index_id = 'TAIEX' AND trade_date <= %s
        ORDER BY trade_date DESC LIMIT %s
        """,
        (end, n),
    )
    return [r["trade_date"] for r in cur.fetchall()]


def _prev_trading_day(cur, d: date) -> date | None:
    cur.execute(
        """
        SELECT MAX(trade_date) AS d FROM tw.index_prices
        WHERE index_id = 'TAIEX' AND trade_date < %s
        """,
        (d,),
    )
    r = cur.fetchone()
    return r["d"] if r and r["d"] else None


def _audit_universe(cur, prev_td: date, audit_date: date) -> list[str]:
    """Stocks the audit must consider: those with attention in the
    bucket window OR those that actually entered disposal on audit_date.
    """
    cur.execute(
        """
        SELECT DISTINCT stock_id FROM tw.stock_alerts
        WHERE alert_type = 'attention'
          AND alert_date >= %s AND alert_date <= %s
        """,
        (prev_td - timedelta(days=60), prev_td),
    )
    universe = {r["stock_id"] for r in cur.fetchall()}
    cur.execute(
        """
        SELECT DISTINCT stock_id FROM tw.stock_alerts
        WHERE alert_type = 'disposal' AND period_start = %s
        """,
        (audit_date,),
    )
    universe |= {r["stock_id"] for r in cur.fetchall()}
    return sorted(universe)


def _actual_disposals(
    cur, audit_date: date
) -> dict[str, str]:
    """Returns {stock_id: reason} for stocks whose disposal period_start
    falls on audit_date."""
    cur.execute(
        """
        SELECT stock_id, reason FROM tw.stock_alerts
        WHERE alert_type = 'disposal' AND period_start = %s
        """,
        (audit_date,),
    )
    return {r["stock_id"]: (r["reason"] or "") for r in cur.fetchall()}


def run_audit(audit_date: date, push: bool = True) -> tuple[int, int, int]:
    """Run the audit for one ``audit_date``. Returns
    ``(n_audited, n_false_pos, n_false_neg)``."""
    with get_cursor(commit=True) as cur:
        prev_td = _prev_trading_day(cur, audit_date)
        if prev_td is None:
            logger.warning("no trading day before %s — skipping", audit_date)
            return 0, 0, 0

        recent = _trading_days_back(cur, prev_td, _LOOKBACK_TD)
        actuals = _actual_disposals(cur, audit_date)
        universe = _audit_universe(cur, prev_td, audit_date)

        false_pos: list[tuple[str, dict]] = []   # predicted but not actual
        false_neg: list[tuple[str, dict, str]] = []  # actual but not predicted
        rows: list[tuple] = []
        for sid in universe:
            triggered, counts = predict_disposal_trigger(cur, sid, prev_td, recent)
            actual_reason = actuals.get(sid)
            actual = actual_reason is not None
            rows.append((
                audit_date, sid, triggered, actual,
                json.dumps(counts), actual_reason,
            ))
            if triggered and not actual:
                false_pos.append((sid, counts))
            elif actual and not triggered:
                false_neg.append((sid, counts, actual_reason or ""))

        # Bulk upsert
        cur.executemany(
            """
            INSERT INTO tw.disposal_prediction_audit
                (audit_date, stock_id, predicted, actual, kuan_counts, actual_reason)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (audit_date, stock_id) DO UPDATE SET
                predicted = EXCLUDED.predicted,
                actual = EXCLUDED.actual,
                kuan_counts = EXCLUDED.kuan_counts,
                actual_reason = EXCLUDED.actual_reason,
                created_at = NOW()
            """,
            rows,
        )

    n_audited = len(universe)
    print(
        f"audit {audit_date}: universe={n_audited}, "
        f"FP={len(false_pos)}, FN={len(false_neg)}"
    )

    if push and (false_pos or false_neg):
        _push_summary(audit_date, false_pos, false_neg)

    return n_audited, len(false_pos), len(false_neg)


def _push_summary(
    audit_date: date,
    false_pos: list[tuple[str, dict]],
    false_neg: list[tuple[str, dict, str]],
) -> None:
    """Telegram-push the day's discrepancies."""
    lines = [f"[Disposal audit {audit_date}]"]
    if false_pos:
        lines.append(f"\n🟠 預測進處置但實際沒有 ({len(false_pos)})")
        for sid, counts in false_pos[:20]:
            top = sorted(counts.items(), key=lambda x: -x[1])[:3]
            summary = ", ".join(f"第{k}款={v}" for k, v in top)
            lines.append(f"  {sid}  {summary}")
        if len(false_pos) > 20:
            lines.append(f"  …其餘 {len(false_pos) - 20} 檔")
    if false_neg:
        lines.append(f"\n🔴 實際進處置但漏抓 ({len(false_neg)})")
        for sid, counts, reason in false_neg[:20]:
            top = sorted(counts.items(), key=lambda x: -x[1])[:3]
            summary = ", ".join(f"第{k}款={v}" for k, v in top) or "—"
            short_reason = reason.split("\n")[0][:60]
            lines.append(f"  {sid}  {summary}  | TWSE: {short_reason}")
        if len(false_neg) > 20:
            lines.append(f"  …其餘 {len(false_neg) - 20} 檔")
    send_sync("\n".join(lines), silent=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("audit_date", nargs="?", help="YYYY-MM-DD (default: today)")
    p.add_argument(
        "--backfill", type=int, default=0,
        help="Audit the last N trading days (inclusive of today)",
    )
    p.add_argument(
        "--no-push", action="store_true",
        help="Skip Telegram push (for backfill / debugging)",
    )
    args = p.parse_args()

    if args.backfill > 0:
        with get_cursor(commit=False) as cur:
            cur.execute(
                """
                SELECT trade_date FROM tw.index_prices
                WHERE index_id = 'TAIEX' AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT %s
                """,
                (date.today(), args.backfill),
            )
            days = [r["trade_date"] for r in cur.fetchall()]
        # audit chronologically
        for d in reversed(days):
            run_audit(d, push=not args.no_push)
        return

    target = (
        date.fromisoformat(args.audit_date) if args.audit_date else date.today()
    )
    run_audit(target, push=not args.no_push)


if __name__ == "__main__":
    main()
