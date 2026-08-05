"""After-close market brief (盤後行情摘要) — one push per trading day.

Data is pulled straight from the exchange APIs, not from Postgres: the nightly
daily_update run lands at 21:30, so nothing for today is in the DB when this
job fires. Nothing here writes to the DB either.

The exchanges do not document when each report goes live, so instead of
guessing a publication time the job polls every 10 minutes from 14:00 and
pushes the first time all five blocks are available. A sentinel file keeps it
to one push per trade date.
"""

from __future__ import annotations

import asyncio
import logging
import os
import unicodedata
from datetime import date, datetime, time as dtime, timedelta, timezone

from scrapers import index_prices as ip
from scrapers import taifex_futures as tfut
from scrapers import taifex_institutional as tinst
from scrapers.price_limits import fetch_twse_limits
from scrapers.taifex_common import download
from telegram_bot.notify import send_sync
from telegram_bot.watchers.morning_brief import _is_trading_day
from utils.http_client import fetch_json_retry

logger = logging.getLogger(__name__)

_TPE_TZ = timezone(timedelta(hours=8))
_WINDOW_OPEN = dtime(hour=14, minute=0)
_WINDOW_CLOSE = dtime(hour=16, minute=30)
_PERIOD_S = 600

JOB_NAME = "afterclose_brief"

# Telegram hard limit is 4096; leave headroom.
_MAX_CHARS = 3800

_SENTINEL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "logs",
    ".afterclose_brief_pushed",
)

_TWSE_MI_INDEX = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"

# Survives a failed sentinel write; reset on process restart, which is fine
# because the sentinel file covers that case.
_pushed_in_process: date | None = None

# 三大法人 report labels -> display label. Investor names are matched by
# prefix because the exchange writes 外資 as 外資及陸資.
_INVESTORS = ("自營商", "投信", "外資")


# ── Time / sentinel ───────────────────────────────────────────────────────


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _in_window(now: datetime) -> bool:
    return _WINDOW_OPEN <= now.time() <= _WINDOW_CLOSE


def _already_pushed(trade_date: date) -> bool:
    if _pushed_in_process == trade_date:
        return True
    try:
        with open(_SENTINEL, encoding="ascii") as f:
            return f.read().strip() == trade_date.isoformat()
    except OSError:
        return False


def _mark_pushed(trade_date: date) -> None:
    # In-process flag first: if the sentinel write fails the job would
    # otherwise re-push on every 10-minute tick for the rest of the window.
    global _pushed_in_process
    _pushed_in_process = trade_date
    try:
        os.makedirs(os.path.dirname(_SENTINEL), exist_ok=True)
        with open(_SENTINEL, "w", encoding="ascii") as f:
            f.write(trade_date.isoformat())
    except OSError as exc:
        logger.error("afterclose_brief: sentinel write failed (%s): %s", _SENTINEL, exc)


# ── Display width helpers (CJK counts as two cells in a monospace block) ──


def _w(s: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)


def _lpad(s: str, n: int) -> str:
    return s + " " * max(0, n - _w(s))


def _rpad(s: str, n: int) -> str:
    return " " * max(0, n - _w(s)) + s


def _num(v: float) -> str:
    """41613.0 -> '41613'; 41603.36 -> '41603.36'."""
    return str(int(v)) if float(v).is_integer() else f"{v:.2f}"


def _signed(v: int) -> str:
    return f"{v:+,}"


def _arrow(v: float) -> str:
    return "▲" if v > 0 else ("▼" if v < 0 else "－")


# ── Fetchers (each returns None when the exchange has not published yet) ──


def _complete(rec: dict | None, *keys: str) -> bool:
    return bool(rec) and all(rec.get(k) is not None for k in keys)


def _fetch_taiex(d: date) -> dict | None:
    records, _, _ = ip.fetch_taiex(d)
    rec = next((r for r in records if r["trade_date"] == d), None)
    if not _complete(rec, "close_price", "change", "change_pct", "turnover"):
        return None
    return {
        "close": rec["close_price"],
        "change": rec["change"],
        "change_pct": rec["change_pct"],
        "turnover_yi": rec["turnover"] / 1e8,
    }


def _fetch_tpex(d: date) -> dict | None:
    records, _, _ = ip.fetch_tpex_index(d)
    rec = next((r for r in records if r["trade_date"] == d), None)
    if not _complete(rec, "close_price", "change", "change_pct"):
        return None

    # fetch_tpex_index leaves turnover NULL (the indexInfo endpoint has no
    # value column); the highlight report carries it, in millions of NTD.
    turnover_yi = None
    data = fetch_json_retry(
        ip.TPEX_HIGHLIGHT_URL,
        params={"date": d.strftime("%Y/%m/%d"), "type": "Daily", "response": "json"},
        validate=lambda j: str(j.get("stat", "")).lower() == "ok",
    )
    if data and str(data.get("date", "")).strip() == d.strftime("%Y%m%d"):
        for table in data.get("tables", []):
            fields = table.get("fields") or []
            idx = next((i for i, f in enumerate(fields) if "總成交值" in str(f)), None)
            rows = table.get("data") or []
            if idx is None or not rows:
                continue
            try:
                turnover_yi = float(str(rows[0][idx]).replace(",", "")) / 100
            except (ValueError, IndexError):
                turnover_yi = None
            break
    if turnover_yi is None:
        return None

    return {
        "close": rec["close_price"],
        "change": rec["change"],
        "change_pct": rec["change_pct"],
        "turnover_yi": turnover_yi,
    }


def _fetch_txf(d: date) -> dict | None:
    """Front-month TX day session: highest-volume non-spread 一般 contract."""
    ymd = d.strftime("%Y/%m/%d")
    text = download(tfut.URL, {
        "down_type": "1", "commodity_id": "all", "commodity_id2": "",
        "queryStartDate": ymd, "queryEndDate": ymd,
    })
    if text is None:
        return None
    records, _, _ = tfut._parse(text)
    candidates = [
        r for r in records
        if r["trade_date"] == d and r["contract"] == "TX"
        and r["session"] == "一般" and "/" not in r["contract_month"]
        and r.get("volume")
        and _complete(r, "close_price", "change", "change_pct")
    ]
    if not candidates:
        return None
    front = max(candidates, key=lambda r: r["volume"])
    return {
        "label": f"台指{front['contract_month'][4:]}",
        "close": front["close_price"],
        "change": front["change"],
        "change_pct": front["change_pct"],
        "volume": front["volume"],
    }


def _inst_net(records: list, product: str) -> dict | None:
    """Net contracts per investor for one product.

    Options rows come split by call/put; the figure quoted for 台指選 is the
    directional net (calls minus puts), so puts flip sign.
    """
    out = {}
    for label in _INVESTORS:
        rows = [
            r for r in records
            if r["product"] == product and r["investor"].startswith(label)
            and r.get("net_volume") is not None
        ]
        if not rows:
            return None
        out[label] = sum(
            -r["net_volume"] if r.get("call_put") == "P" else r["net_volume"]
            for r in rows
        )
    return out


def _fetch_inst(d: date) -> dict | None:
    ymd = d.strftime("%Y/%m/%d")
    params = {"queryStartDate": ymd, "queryEndDate": ymd, "commodityId": ""}

    fut_text = download(tinst.FUT_URL, params)
    opt_text = download(tinst.OPT_URL, params)
    if fut_text is None or opt_text is None:
        return None
    fut, _, _ = tinst._parse_fut(fut_text)
    opt, _, _ = tinst._parse_opt(opt_text)
    fut = [r for r in fut if r["trade_date"] == d]
    opt = [r for r in opt if r["trade_date"] == d]

    blocks = {
        "台指期": _inst_net(fut, "臺股期貨"),
        "台指選": _inst_net(opt, "臺指選擇權"),
        "小台指期": _inst_net(fut, "小型臺指期貨"),
    }
    if any(v is None for v in blocks.values()):
        return None
    return blocks


def _fetch_limits(d: date) -> dict | None:
    """TWSE common stocks closing exactly at their limit price."""
    limits, _, _ = fetch_twse_limits(d)
    limits = {r["stock_id"]: r for r in limits if r["security_type"] == "STOCK"}
    if not limits:
        return None

    data = fetch_json_retry(
        _TWSE_MI_INDEX,
        params={"date": d.strftime("%Y%m%d"), "type": "ALLBUT0999", "response": "json"},
        validate=lambda j: j.get("stat") == "OK",
    )
    if not data or str(data.get("date", "")).strip() != d.strftime("%Y%m%d"):
        return None

    up, down = [], []
    for table in data.get("tables", []):
        fields = [str(f) for f in (table.get("fields") or [])]
        if not fields or "證券代號" not in fields[0]:
            continue
        try:
            close_idx = fields.index("收盤價")
        except ValueError:
            continue
        for row in table.get("data") or []:
            rec = limits.get(str(row[0]).strip())
            if rec is None:
                continue
            try:
                close = float(str(row[close_idx]).replace(",", ""))
            except ValueError:
                continue
            if rec["limit_up"] is not None and close == rec["limit_up"]:
                up.append(rec["name"])
            elif rec["limit_down"] is not None and close == rec["limit_down"]:
                down.append(rec["name"])
        break
    else:
        return None

    return {"up": up, "down": down}


# ── Message ───────────────────────────────────────────────────────────────


def _quote_line(label: str, close: float, change: float, pct: float, tail: str) -> tuple:
    return (label, _num(close), _arrow(change), _num(abs(change)), f"({pct:+.2f}%)", tail)


def _build_quotes(taiex: dict, txf: dict, tpex: dict) -> str:
    rows = [
        _quote_line("台股", taiex["close"], taiex["change"], taiex["change_pct"],
                    f"{taiex['turnover_yi']:,.2f}億"),
        _quote_line(txf["label"], txf["close"], txf["change"], txf["change_pct"],
                    f"{txf['volume']:,}口"),
        _quote_line("櫃買", tpex["close"], tpex["change"], tpex["change_pct"],
                    f"{tpex['turnover_yi']:,.2f}億"),
    ]
    widths = [max(_w(r[i]) for r in rows) for i in range(6)]
    lines = []
    for r in rows:
        lines.append(
            f"{_lpad(r[0], widths[0])}  {_rpad(r[1], widths[1])}  "
            f"{r[2]}{_rpad(r[3], widths[3])}  {_rpad(r[4], widths[4])}  "
            f"{_rpad(r[5], widths[5])}"
        )
    return "\n".join(lines)


def _build_inst(inst: dict) -> str:
    rows = [
        (name, *[_signed(inst[name][k]) for k in _INVESTORS])
        for name in ("台指期", "台指選", "小台指期")
    ]
    widths = [max(_w(r[i]) for r in rows) for i in range(4)]
    lines = []
    for r in rows:
        cells = " ".join(
            f"{label} {_rpad(r[i + 1], widths[i + 1])}"
            for i, label in enumerate(_INVESTORS)
        )
        lines.append(f"{_lpad(r[0], widths[0])}  {cells}")
    return "\n".join(lines)


def _limit_lines(title: str, names: list[str], budget: int) -> list[str]:
    """'<title> N (a、b、…)', broken into 續 lines when the name list is long."""
    head = f"{title} {len(names):>2}"
    if not names:
        return [head]
    lines, prefix, cur = [], head, []
    for name in names:
        candidate = cur + [name]
        if cur and len(f"{prefix} ({'、'.join(candidate)})") > budget:
            lines.append(f"{prefix} ({'、'.join(cur)})")
            prefix, cur = f"{title}（續）", [name]
        else:
            cur = candidate
    lines.append(f"{prefix} ({'、'.join(cur)})")
    return lines


def _limit_sections(limits: dict, budget: int) -> list[str]:
    lines = (_limit_lines("漲停家數 (+10%)", limits["up"], budget)
             + _limit_lines("跌停家數 (-10%)", limits["down"], budget))
    block = "\n".join(["上市", ""] + lines)
    return [block] if len(block) <= budget else ["上市"] + lines


def _build_messages(d: date, taiex: dict, txf: dict, tpex: dict,
                    inst: dict, limits: dict) -> list[str]:
    header = f"盤後行情 {d:%Y-%m-%d}"
    # Every message carries the header (or its 續 variant) plus a blank line.
    budget = _MAX_CHARS - len(header) - 8
    sections = [
        _build_quotes(taiex, txf, tpex),
        "日盤\n\n" + _build_inst(inst),
        *_limit_sections(limits, budget),
    ]

    messages, cur = [], header
    for section in sections:
        if len(cur) + 2 + len(section) <= _MAX_CHARS:
            cur = f"{cur}\n\n{section}"
        else:
            messages.append(cur)
            cur = f"{header}（續）\n\n{section}"
    messages.append(cur)
    return messages


# ── Job ───────────────────────────────────────────────────────────────────


def _run_check(*, respect_gates: bool, force: bool = False,
               dry_run: bool = False) -> tuple[list[str], str]:
    now = _now_tpe()
    today = now.date()
    if respect_gates:
        if not _is_trading_day(today):
            return [], "skipped: non-trading day"
        if not _in_window(now):
            return [], "skipped: outside window"
    if not force and not dry_run and _already_pushed(today):
        return [], "skipped: already pushed"

    blocks = {}
    for name, fetch in (
        ("taiex", _fetch_taiex), ("txf", _fetch_txf), ("tpex", _fetch_tpex),
        ("inst", _fetch_inst), ("limits", _fetch_limits),
    ):
        blocks[name] = fetch(today)
        if blocks[name] is None:
            return [], f"waiting: {name} not published yet"

    messages = _build_messages(today, blocks["taiex"], blocks["txf"], blocks["tpex"],
                               blocks["inst"], blocks["limits"])
    if dry_run:
        return messages, f"dry-run: {len(messages)} message(s), not sent"

    ok = all(send_sync(m) for m in messages)
    if ok:
        _mark_pushed(today)
    return messages, f"sent={len(messages)}, send_ok={ok}"


async def check_once(context) -> None:  # noqa: ANN001 — PTB context
    try:
        _, status = await asyncio.to_thread(_run_check, respect_gates=True)
        logger.info("afterclose_brief: %s", status)
    except Exception as exc:
        logger.exception("afterclose_brief crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; afterclose_brief not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    jq.run_repeating(check_once, interval=_PERIOD_S, first=10, name=JOB_NAME)
    logger.info("Scheduled %s every %ds", JOB_NAME, _PERIOD_S)


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="After-close market brief")
    parser.add_argument("--dry-run", action="store_true", help="print, do not send")
    parser.add_argument("--once", action="store_true",
                        help="send now, ignoring window and sentinel")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    messages, status = _run_check(
        respect_gates=not (args.dry_run or args.once),
        force=args.once and not args.dry_run,
        dry_run=args.dry_run,
    )
    print(f"[afterclose_brief] {status}")
    for msg in messages:
        print("-" * 60)
        print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
