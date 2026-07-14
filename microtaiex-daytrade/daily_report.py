"""Post-close P&L summary for the paper-trading engines, pushed to Telegram.

Each session force-closes before its own close (day 13:45 / night 05:00), so by
the time this runs every round-trip of the session is already flushed to the
TradeLog CSV — reading the CSV is enough, and this stays out of the live COM
loop entirely.

    python daily_report.py day              # today's day session
    python daily_report.py night            # the night session that just ended
    python daily_report.py day 2026-07-14   # backfill a specific date

`night` is anchored on the calendar day it ENDS: run at 05:10 on the 15th, it
reports the session that opened 15:00 on the 14th.
"""
from __future__ import annotations

import csv
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from backtest.replay import CostModel     # noqa: E402
from config_env import load_dotenv        # noqa: E402
from core import clock                    # noqa: E402
from notify import TelegramNotifier       # noqa: E402

# Mirrors the InvestMicroPaper service cmdline (`--paper --gated --masha`):
# --gated SWITCHES the composite engine's log file rather than adding an engine,
# so reports/paper_trades.csv is dormant and deliberately not listed here.
ENGINES = [
    ("composite", "reports/paper_trades_gated.csv"),
    ("麻紗", "reports/paper_trades_masha.csv"),
]


def _in_session(ts: datetime, session: clock.Session, anchor: date) -> bool:
    """Does `ts` fall in the `session` of trading day `anchor`?

    Day sessions sit inside one calendar day. A night session straddles
    midnight: it opens 15:00 on `anchor - 1` and closes 05:00 on `anchor`.
    """
    if clock.session_of(ts) is not session:
        return False
    if session is clock.Session.DAY:
        return ts.date() == anchor
    return (
        (ts.date() == anchor - timedelta(days=1) and ts.time() >= clock.NIGHT_OPEN)
        or (ts.date() == anchor and ts.time() < clock.NIGHT_CLOSE)
    )


def _load_session_trades(
    path: Path, session: clock.Session, anchor: date, cost: CostModel
) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            try:
                exit_ts = datetime.fromisoformat(r["exit_ts"])
            except (ValueError, TypeError, KeyError):
                continue
            if not _in_session(exit_ts, session, anchor):
                continue
            ep, xp, lot = float(r["entry_price"]), float(r["exit_price"]), int(r["lot"])
            gross = float(r["gross_ntd"])
            out.append({
                "side": r["side"],
                "points": float(r["points"]),
                "gross": gross,
                "net": gross - cost.round_trip_cost(ep, xp, lot),
                "signal": r.get("signal") or "",
            })
    return out


def _engine_block(label: str, trades: list[dict]) -> tuple[list[str], float]:
    if not trades:
        return [f"〔{label}〕— 無交易"], 0.0

    n = len(trades)
    wins = [t for t in trades if t["points"] > 0]
    gross = sum(t["gross"] for t in trades)
    net = sum(t["net"] for t in trades)
    best = max(trades, key=lambda t: t["net"])
    worst = min(trades, key=lambda t: t["net"])

    mark = "✅" if net > 0 else ("❌" if net < 0 else "➖")
    lines = [
        f"〔{label}〕{mark} {net:+,.0f} 元",
        f"　{n} 筆・勝率 {len(wins) / n * 100:.0f}%・毛 {gross:+,.0f}・成本 {gross - net:,.0f}",
    ]
    if n > 1:
        lines.append(
            f"　最佳 {best['net']:+,.0f}（{best['signal']}）"
            f"　最差 {worst['net']:+,.0f}（{worst['signal']}）"
        )
    return lines, net


def build_report(session: clock.Session, anchor: date) -> str:
    cost = CostModel()
    title = "日盤" if session is clock.Session.DAY else "夜盤"
    if session is clock.Session.DAY:
        span = f"{anchor:%m-%d} 08:45-13:45"
    else:
        span = f"{anchor - timedelta(days=1):%m-%d} 15:00 → {anchor:%m-%d} 05:00"

    parts = [f"📒 微台當沖結算　{title}", span, ""]
    total = 0.0
    for label, rel_path in ENGINES:
        lines, net = _engine_block(
            label,
            _load_session_trades(_ROOT / rel_path, session, anchor, cost),
        )
        parts.extend(lines)
        total += net

    verdict = "賺" if total > 0 else ("賠" if total < 0 else "平手")
    parts.append("")
    parts.append(f"合計 {total:+,.0f} 元（{verdict}）")
    return "\n".join(parts)


def _has_session(session: clock.Session, anchor: date) -> bool:
    """Weekends only — a national holiday still reports 無交易, same as the
    morning brief. TAIFEX day sessions run Mon-Fri; the night session that ends
    on `anchor` opened the previous weekday evening."""
    d = anchor if session is clock.Session.DAY else anchor - timedelta(days=1)
    return d.weekday() < 5


def main(argv) -> int:
    load_dotenv()
    which = argv[1].lower() if len(argv) > 1 else "day"
    if which not in ("day", "night"):
        print(f"usage: python daily_report.py [day|night] [YYYY-MM-DD]")
        return 2
    session = clock.Session.DAY if which == "day" else clock.Session.NIGHT
    anchor = date.fromisoformat(argv[2]) if len(argv) > 2 else datetime.now().date()

    if not _has_session(session, anchor):
        print(f"[daily_report] {which} {anchor}: no session (weekend), skipped")
        return 0

    msg = build_report(session, anchor)
    print(msg)
    TelegramNotifier().send(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
