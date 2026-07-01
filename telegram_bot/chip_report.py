"""Weekly chip-model (集保大戶選股) report for Telegram.

chip_picks.json is regenerated every Saturday by daily_update.update_chip_only
after TDCC publishes the weekly shareholder distribution. This composes the
Chinese weekly report pushed at that point (and available on demand via /chip),
highlighting this week's NEW entrants -- tickers in weeks[0] but not weeks[1] --
on the long and short lists, which is the actionable week-over-week signal.

CLI:
    python -m telegram_bot.chip_report          # print the current report
    python -m telegram_bot.chip_report --send   # also push via Telegram
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _default_json_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "frontend" / "public" / "data" / "chip_picks.json"
    )


def _fmt_rows(rows: list[dict], max_n: int) -> list[str]:
    lines = [f"  {r['ticker']} {r.get('name') or ''}".rstrip() for r in rows[:max_n]]
    extra = len(rows) - max_n
    if extra > 0:
        lines.append(f"  …還有 {extra} 檔")
    return lines


def _full_rows(rows: list[dict], new_set: set[str]) -> list[str]:
    return [
        f"  {r['rank']} {'★' if r['ticker'] in new_set else ''}{r['ticker']} "
        f"{r.get('name') or ''}".rstrip()
        for r in rows
    ]


def build_chip_report(
    json_path: Path | None = None, max_per_side: int = 10, full: bool = False
) -> str | None:
    """Compose the weekly chip-pick report, or None if there is no data
    (callers fall back to a plain 'updated' notice).

    full=False (default, Saturday push): concise -- this week's NEW entrants
        only, capped at max_per_side.
    full=True (/chip on demand): the complete current-week top lists, with new
        entrants starred.
    """
    path = Path(json_path) if json_path else _default_json_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    weeks = data.get("weeks") or []
    if not weeks:
        return None

    latest = data.get("latest_date") or weeks[0].get("date")
    header = f"[集保大戶選股週報] {latest}"
    cur = weeks[0]

    # New entrants: in this week's list but not last week's (empty on week 1).
    new_long_set: set[str] = set()
    new_short_set: set[str] = set()
    if len(weeks) >= 2:
        prev_long = {r["ticker"] for r in weeks[1].get("long", [])}
        prev_short = {r["ticker"] for r in weeks[1].get("short", [])}
        new_long_set = {r["ticker"] for r in cur.get("long", []) if r["ticker"] not in prev_long}
        new_short_set = {r["ticker"] for r in cur.get("short", []) if r["ticker"] not in prev_short}

    if full:
        parts = [header]
        for emoji, side_zh, rows, new_set in (
            ("📈", "做多", cur.get("long", []), new_long_set),
            ("📉", "做空", cur.get("short", []), new_short_set),
        ):
            if rows:
                parts.append(f"{emoji} {side_zh} top{len(rows)}（★=本週新進）")
                parts.extend(_full_rows(rows, new_set))
        parts.append("（完整名單見前端 集保精選）")
        return "\n".join(parts)

    # Concise new-entrants mode (Saturday push / default).
    if len(weeks) >= 2:
        new_long = [r for r in cur.get("long", []) if r["ticker"] in new_long_set]
        new_short = [r for r in cur.get("short", []) if r["ticker"] in new_short_set]
        long_title = f"📈 做多新進榜（{len(new_long)}）"
        short_title = f"📉 做空新進榜（{len(new_short)}）"
        if not new_long and not new_short:
            return f"{header}\n本週無新進榜，名單同上週。\n（完整名單見前端 集保精選）"
    else:
        # First week ever -- no previous week to diff, list the top picks.
        new_long = cur.get("long", [])
        new_short = cur.get("short", [])
        long_title = f"📈 做多名單（前 {max_per_side}）"
        short_title = f"📉 做空名單（前 {max_per_side}）"

    parts = [header]
    if new_long:
        parts.append(long_title)
        parts.extend(_fmt_rows(new_long, max_per_side))
    if new_short:
        parts.append(short_title)
        parts.extend(_fmt_rows(new_short, max_per_side))
    parts.append("（完整名單見前端 集保精選）")
    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # console may be cp950 on Windows
    except (AttributeError, ValueError):
        pass
    report = build_chip_report(full="--full" in argv)
    if report is None:
        print("no chip_picks data", file=sys.stderr)
        return 1
    print(report)
    if "--send" in argv:
        from telegram_bot.notify import send_sync
        return 0 if send_sync(report) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
