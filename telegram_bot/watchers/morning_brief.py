"""Daily 08:00 TPE 早安管家 digest.

Runs once per weekday morning. Builds a watchlist-scoped brief from the
daily EOD JSON snapshots refreshed by daily_update.exe the previous evening:

  📊 評分變動    notable rank movements in tw.score_snapshot (top-300)
  📈 訊號命中    6-signal hits ∩ watchlist
  🔄 部位進出    yesterday's strategy entries + exits ∩ watchlist
  ⚠️ 處置預警    near-disposal alerts via _get_disposal_status

Sections are silently dropped when empty; when every section is empty the
brief is suppressed entirely (no "今早無重點" filler).

CLI (force-build, ignores trading-day gate):
    python -m telegram_bot.watchers.morning_brief
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from intraday.watchlist import load_tw_watchlist
from telegram_bot.notify import send_sync
from telegram_bot.push_intraday_signals import _SIGNAL_LABEL, _SIGNAL_ORDER

logger = logging.getLogger(__name__)

_TPE_TZ = timezone(timedelta(hours=8))
_PUSH_TIME = dtime(hour=8, minute=0)

JOB_NAME = "morning_brief"

# Rank-change event thresholds
_ENTER_TOP_RANK = 50
_RANK_DELTA_NOTABLE = 30

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SCORES_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "scores.json"
_OPERATIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "operations.json"
_POSITIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "positions.json"


# ── Helpers ───────────────────────────────────────────────────────────────


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _is_trading_day(d: date) -> bool:
    return d.weekday() < 5


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _weekday_zh(d: date) -> str:
    return "週" + "一二三四五六日"[d.weekday()]


# ── Section builders ──────────────────────────────────────────────────────


def _build_section_score_changes(
    scores: dict | None, watchlist: set[str]
) -> list[str] | None:
    """Flag watchlist tickers that newly entered the top-50 OR moved ≥30 ranks
    in either direction. Capped at top-300 (daily snapshot limit)."""
    if not scores:
        return None
    lines: list[str] = []
    for side, label in (("long", "多"), ("short", "空")):
        for e in scores.get(side, []):
            if e.get("ticker") not in watchlist:
                continue
            rank = e.get("rank")
            prev = e.get("prev_rank")
            delta = e.get("rank_delta")
            is_new = e.get("is_new")

            event = None
            if rank is not None and rank <= _ENTER_TOP_RANK and (
                is_new or (prev is not None and prev > _ENTER_TOP_RANK)
            ):
                event = f"進入{label}前 {_ENTER_TOP_RANK}"
            elif isinstance(delta, (int, float)) and abs(delta) >= _RANK_DELTA_NOTABLE:
                direction = "↑" if delta > 0 else "↓"
                event = f"{label}側 {direction}{abs(int(delta))} 名"

            if event:
                lines.append(
                    f"  {e.get('ticker')} {e.get('name', '')} {label}#{rank}  {event}"
                )
    return lines or None


def _build_section_signals(
    operations: dict | None, watchlist: set[str]
) -> list[str] | None:
    if not operations:
        return None
    sigs = operations.get("signals", {})
    lines: list[str] = []
    for kind in _SIGNAL_ORDER:
        hits = [e for e in sigs.get(kind, []) if e.get("ticker") in watchlist]
        if not hits:
            continue
        label = _SIGNAL_LABEL.get(kind, kind)
        body = " / ".join(f"{e['ticker']} {e.get('name', '')}" for e in hits)
        lines.append(f"  {label}：{body}")
    return lines or None


def _build_section_positions(
    positions: dict | None, watchlist: set[str], snapshot_date: str | None
) -> list[str] | None:
    if not positions:
        return None
    lines: list[str] = []
    # Yesterday's new entries — active rows whose entry_date matches the snapshot.
    for bucket, side_zh in (("long", "多"), ("short", "空")):
        for p in positions.get(bucket, []):
            if p.get("ticker") not in watchlist:
                continue
            if p.get("entry_date") != snapshot_date:
                continue
            tier = p.get("entry_tier", "")
            entry_price = p.get("entry_price")
            price_str = f"{float(entry_price):g}" if entry_price is not None else "?"
            lines.append(
                f"  🆕 {p['ticker']} {p.get('name', '')} {side_zh}單"
                f"  @ {price_str}（tier={tier}）"
            )
    # Yesterday's exits — daily snapshot's exited_* buckets only contain rows
    # whose exit_date == snapshot_date, so no extra filter needed.
    for bucket, side_zh in (("exited_long", "多"), ("exited_short", "空")):
        for p in positions.get(bucket, []):
            if p.get("ticker") not in watchlist:
                continue
            pnl = p.get("pnl_pct")
            pnl_str = f"{float(pnl):+.2f}%" if pnl is not None else "?"
            reason = p.get("exit_reason") or ""
            reason_part = f"（{reason}）" if reason else ""
            lines.append(
                f"  🟡 {p['ticker']} {p.get('name', '')} {side_zh}單 出場 {pnl_str}{reason_part}"
            )
    return lines or None


def _build_section_disposal(watchlist: set[str]) -> list[str] | None:
    """Reuse score.py's _get_disposal_status. Only keep the loud rows
    (🔴 in disposal / 進處置 / 🟠 1 次即進) so the brief stays focused.
    Names are batch-fetched from tw.stocks so each row shows
    "<ticker> <name>" instead of just the ticker."""
    try:
        from telegram_bot.handlers.score import _get_disposal_status
    except Exception:
        return None
    try:
        from telegram_bot.handlers._data_freshness import detect_state
        freshness = detect_state()
    except Exception:
        freshness = None

    # Batch name lookup so we don't hit DB per ticker.
    names: dict[str, str] = {}
    try:
        from db.connection import get_cursor
        with get_cursor(commit=False) as cur:
            cur.execute(
                "SELECT stock_id, name FROM tw.stocks WHERE stock_id = ANY(%s)",
                (list(watchlist),),
            )
            for r in cur.fetchall():
                names[r["stock_id"]] = r["name"] or ""
    except Exception as e:
        logger.warning("morning_brief: stock name lookup failed: %s", e)

    lines: list[str] = []
    for t in sorted(watchlist):
        try:
            status = _get_disposal_status(t, freshness, allow_refresh=False)
        except Exception as e:
            logger.warning("morning_brief: disposal status failed for %s: %s", t, e)
            continue
        if not status:
            continue
        if "🔴" in status or "🟠" in status:
            name = names.get(t, "")
            label = f"{t} {name}".rstrip()
            lines.append(f"  {label}  {status}")
    return lines or None


def _load_ftse_latest() -> dict | None:
    """Read the latest 富台/台指期夜盤 row from tw.ftse_taiwan.

    The row is refreshed at 07:55 TPE by the scheduled FtseTxfUpdate job, 5
    minutes before this 08:00 brief, so we just read it (no second fetch). If
    that job didn't run or failed, this returns the last stored row — the
    section's 夜盤資料 timestamp then reveals the data is not from today."""
    try:
        from db.connection import get_cursor
        with get_cursor(commit=False) as cur:
            cur.execute(
                """
                SELECT ftse_now, ftse_base, pct_change, taiex_ref_date,
                       taiex_ref_close, theoretical_taiex, captured_at,
                       txf_now, txf_pct_change, txf_captured_at
                FROM tw.ftse_taiwan
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
            return cur.fetchone()
    except Exception as e:
        logger.warning("morning_brief: ftse DB read failed: %s", e)
        return None


def _build_section_ftse_taiwan(row: dict | None) -> list[str] | None:
    """富台(SGX)換算理論 TAIEX + 台指期(TXF)夜盤,估今日開盤落點。"""
    if not row or row.get("theoretical_taiex") is None:
        return None

    def _pct(v) -> str:
        return f"{float(v) * 100:+.2f}%" if v is not None else "—"

    def _num(v) -> str:
        return f"{float(v):,.2f}" if v is not None else "—"

    # Staleness: the 富台 quote is meant to carry the overnight session, which
    # opens 15:00 TPE on the TW cash-close day. A captured_at before that means
    # cnyes froze the quote at the day session (or earlier) and the night data
    # never arrived — flag it so the value isn't misread as a live estimate.
    stale = False
    captured = row.get("captured_at")
    ref_date = row.get("taiex_ref_date")
    if captured is not None and ref_date is not None:
        try:
            night_start = datetime(
                ref_date.year, ref_date.month, ref_date.day, 15, 0, tzinfo=_TPE_TZ
            )
            stale = captured.astimezone(_TPE_TZ) < night_start
        except Exception:
            pass

    warn = " ⚠️ 富台夜盤停更" if stale else ""
    lines = [
        f"  富台 → 理論 TAIEX：{_num(row.get('theoretical_taiex'))}"
        f"（{_pct(row.get('pct_change'))}）{warn}"
    ]
    if row.get("txf_now") is not None:
        lines.append(
            f"  台指期夜盤：{_num(row.get('txf_now'))}（{_pct(row.get('txf_pct_change'))}）"
        )
    if row.get("taiex_ref_close") is not None:
        lines.append(
            f"  台股收盤：{_num(row.get('taiex_ref_close'))}"
            f"（{row.get('taiex_ref_date')}）"
        )
    captured = row.get("captured_at")
    if captured is not None:
        try:
            lines.append(f"  夜盤資料：{captured.astimezone(_TPE_TZ):%m/%d %H:%M}")
        except Exception:
            pass
    return lines


# ── Composer ──────────────────────────────────────────────────────────────


def _build_message(today: date, snapshot_date: str, sections: dict) -> str:
    header = (
        f"[早安管家] {today} {_weekday_zh(today)}\n"
        f"資料日：{snapshot_date}"
    )
    parts = [header]
    section_titles = (
        ("ftse_taiwan", "🌏 夜盤估開盤"),
        ("scores", "📊 評分變動"),
        ("signals", "📈 昨日訊號（追蹤股）"),
        ("positions", "🔄 昨日部位進出（追蹤股）"),
        ("disposal", "⚠️ 處置預警（追蹤股）"),
    )
    for key, title in section_titles:
        body = sections.get(key)
        if body:
            parts.append(f"{title}\n" + "\n".join(body))
    return "\n\n".join(parts)


def _run_check(*, respect_gates: bool) -> tuple[str | None, str]:
    today = _now_tpe().date()
    if respect_gates and not _is_trading_day(today):
        return None, "skipped: non-trading day"

    watchlist = set(load_tw_watchlist(include_etf=True))
    if not watchlist:
        return None, "skipped: watchlist empty"

    scores = _load_json(_SCORES_JSON)
    operations = _load_json(_OPERATIONS_JSON)
    positions = _load_json(_POSITIONS_JSON)
    snapshot_date = (
        (scores or {}).get("snapshot_date")
        or (operations or {}).get("snapshot_date")
        or (positions or {}).get("snapshot_date")
        or "?"
    )

    sections = {
        "ftse_taiwan": _build_section_ftse_taiwan(_load_ftse_latest()),
        "scores": _build_section_score_changes(scores, watchlist),
        "signals": _build_section_signals(operations, watchlist),
        "positions": _build_section_positions(positions, watchlist, snapshot_date),
        "disposal": _build_section_disposal(watchlist),
    }

    populated = sum(1 for v in sections.values() if v)
    if populated == 0:
        return None, "no notable events; suppressed"

    msg = _build_message(today, snapshot_date, sections)
    ok = send_sync(msg) if respect_gates else True
    return msg, f"sections={populated}, send_ok={ok}"


async def check_once(context) -> None:  # noqa: ANN001 — PTB context
    try:
        msg, status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("morning_brief: %s", status)
    except Exception as exc:
        logger.exception("morning_brief crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; morning_brief not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    # `days` semantics differ across PTB versions; safer to run every day and
    # let _run_check's trading-day gate filter weekends.
    jq.run_daily(
        check_once,
        time=_PUSH_TIME.replace(tzinfo=_TPE_TZ),
        name=JOB_NAME,
    )
    logger.info(
        "Scheduled %s daily at %s TPE (weekend self-gated)",
        JOB_NAME,
        _PUSH_TIME.strftime("%H:%M"),
    )


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    msg, status = _run_check(respect_gates=False)
    print(f"[morning_brief] {status}")
    if msg:
        print()
        print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
