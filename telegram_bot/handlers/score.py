"""`/score <ticker>` — comprehensive single-stock snapshot.

Combines four sources for one ticker:
  1. ScoreBoard pct + rank + 3-day trend (scores_intraday.json)
  2. Today's 6-signal-factory hits (operations_intraday.json)
  3. ETF holdings + recent share / weight changes (tw.etf_holdings*)
  4. 注意 / 處置 alerts in the last 30 days (tw.stock_alerts)

Sections are best-effort: if a section has no data, it is silently omitted.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from db.connection import get_cursor
from telegram_bot.auth import restricted
from telegram_bot.handlers._attention_predict_intraday import (
    closest_untriggered_threshold,
    consec_rule1_eligible_days,
    format_today_thresholds,
    predict_today_attention,
)
from telegram_bot.handlers._data_freshness import DataState, Freshness, detect_state
from telegram_bot.push_intraday_signals import _SIGNAL_LABEL, _SIGNAL_ORDER

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SCORES_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "scores_intraday.json"
_OPERATIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "operations_intraday.json"
_POSITIONS_JSON = _REPO_ROOT / "frontend" / "public" / "data" / "positions_intraday.json"

# (bucket_key, side, is_exited_today) — positions_intraday.json splits
# active vs today-exited into separate keys per side.
_POS_BUCKETS = (
    ("long", "long", False),
    ("short", "short", False),
    ("exited_long", "long", True),
    ("exited_short", "short", True),
)

# How far back to look for ETF diffs / alerts
_DIFF_LOOKBACK_DAYS = 14

# TWSE 處置 trigger rules (ref: 處置作業要點 第6條).
# Conditions 3/4 explicit; (5,5) used both as summary indicator and as the
# fastest path to "明日 1 次即進" (4/5 means tomorrow's attention triggers).
_DISPOSAL_RULES = (
    (5, 5, "連續5日"),
    (10, 6, "10日6次"),
    (30, 12, "30日12次"),
)


def _parse_auction_interval(measure: str | None) -> str | None:
    """Extract the 集合競價 interval ("5分盤" / "20分盤") from a disposal
    measure text. Supports both the long-form measure (含「約每N分鐘」) and
    the short-form ("第一次處置" / "第二次處置")."""
    if not measure:
        return None
    m = re.search(r"約每(\d+)分鐘", measure)
    if m:
        return f"{m.group(1)}分盤"
    if "第二次處置" in measure or "第三次處置" in measure:
        return "20分盤"
    if "第一次處置" in measure:
        return "5分盤"
    return None

_CHANGE_TYPE_ZH = {
    "added": "新增",
    "removed": "剔除",
    "increased": "加碼",
    "decreased": "減碼",
}

# Taiwan convention: long/bullish = red, short/bearish = green
_RED = "🔴"
_GREEN = "🟢"
_YELLOW = "🟡"
_ORANGE = "🟠"
_SIREN = "🚨"


def _compact_threshold(s: str | None, prefix: str = "") -> str:
    """Convert closest_untriggered_threshold's long form (e.g. '離 §X 還差
    1.45 元（漲到 145.86）') into a compact inline form like '今日需漲到
    145.86' (when prefix='今日需'). Returns '' when no extractable pattern."""
    if not s:
        return ""
    m = re.search(r"（(漲到|跌到)\s*([\d.]+)", s)
    if not m:
        return ""
    direction = "漲到" if "漲" in m.group(1) else "跌到"
    price = m.group(2)
    try:
        price = f"{float(price):g}"
    except ValueError:
        pass
    return f"{prefix}{direction} {price}"

_CHANGE_TYPE_COLOR = {
    "added": _RED,
    "removed": _GREEN,
    "increased": _RED,
    "decreased": _GREEN,
}

# Signal kinds: bullish kinds get red, bearish kinds get green
_SIGNAL_COLOR = {
    "pick": _RED,         # 撿便宜（看多）
    "buy": _RED,          # 做多
    "sell_flee": _RED,    # 空單逃命（變多）
    "touch": _GREEN,      # 摸頭（看空）
    "sell": _GREEN,       # 做空
    "buy_flee": _GREEN,   # 多單逃命（變空）
}



# ── JSON loaders ──────────────────────────────────────────────────────────


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _find_in_ranking(entries: list[dict], ticker: str) -> dict | None:
    for e in entries:
        if e.get("ticker") == ticker:
            return e
    return None


def _get_turnover_rank_and_price(ticker: str) -> tuple[int | None, int | None, float | None]:
    """Live lookup of this ticker's (turnover rank, total ranked, latest price).

    Rank is computed against the most recent score_snapshot_intraday snapshot's
    full alive universe (~990 stocks). Price comes from tw.intraday_quotes which
    holds today's last tick (and is the only fresh source before EOD lands)."""
    rank: int | None = None
    total: int | None = None
    price: float | None = None
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT rk, total FROM (
                SELECT stock_id,
                       RANK() OVER (ORDER BY turnover DESC NULLS LAST) AS rk,
                       COUNT(*) OVER () AS total
                FROM tw.score_snapshot_intraday
                WHERE (snapshot_date, snapshot_time) = (
                    SELECT snapshot_date, snapshot_time FROM tw.score_snapshot_intraday
                    ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
                ) AND side = 'long'
            ) x WHERE stock_id = %s
        """, (ticker,))
        r = cur.fetchone()
        if r:
            rank = int(r["rk"])
            total = int(r["total"])

        cur.execute("""
            SELECT last_price FROM tw.intraday_quotes
            WHERE stock_id = %s
            ORDER BY updated_at DESC LIMIT 1
        """, (ticker,))
        r = cur.fetchone()
        if r and r["last_price"] is not None:
            price = float(r["last_price"])
    return rank, total, price


def _lookup_db_sides(ticker: str) -> dict[str, dict | None]:
    """Fallback for tickers outside the JSON top-300 slice.

    Reads the most recent (snapshot_date, snapshot_time) tuple straight from
    tw.score_snapshot_intraday — which now stores the full alive universe per
    side — and returns a dict shaped like the JSON entries with an extra
    ``off_ranking`` flag so the formatter can mark these rows."""
    out: dict[str, dict | None] = {"long": None, "short": None}
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT s.side, s.rank, s.total_pct, s.turnover,
                   s.is_new, s.prev_rank, s.rank_delta,
                   s.pct_d1, s.pct_d2, s.pct_d3,
                   st.name, st.market
            FROM tw.score_snapshot_intraday s
            JOIN tw.stocks st ON st.stock_id = s.stock_id
            WHERE s.stock_id = %s
              AND (s.snapshot_date, s.snapshot_time) = (
                  SELECT snapshot_date, snapshot_time
                  FROM tw.score_snapshot_intraday
                  ORDER BY snapshot_date DESC, snapshot_time DESC
                  LIMIT 1
              )
        """, (ticker,))
        for r in cur.fetchall():
            out[r["side"]] = {
                "rank": r["rank"],
                "ticker": ticker,
                "name": r["name"],
                "market": r["market"],
                "total_pct": float(r["total_pct"]),
                "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
                "is_new": r["is_new"],
                "prev_rank": r["prev_rank"],
                "rank_delta": r["rank_delta"],
                "pct_d1": float(r["pct_d1"]) if r["pct_d1"] is not None else None,
                "pct_d2": float(r["pct_d2"]) if r["pct_d2"] is not None else None,
                "pct_d3": float(r["pct_d3"]) if r["pct_d3"] is not None else None,
                "off_ranking": True,
            }
    return out


# ── Signal lookup ─────────────────────────────────────────────────────────


def _get_positions(ticker: str, positions: dict | None) -> list[dict]:
    """Collect all position rows for this ticker across the 4 buckets in
    positions_intraday.json. Each returned dict carries the original JSON
    fields plus ``_side`` ('long'/'short') and ``_is_exited`` (today)."""
    if not positions:
        return []
    out: list[dict] = []
    for bucket_key, side, is_exited in _POS_BUCKETS:
        for p in positions.get(bucket_key, []):
            if p.get("ticker") == ticker:
                out.append({**p, "_side": side, "_is_exited": is_exited})
    return out


def _get_signal_hits(ticker: str, operations: dict | None) -> list[tuple[str, str, int | None]]:
    """Return [(kind, zh_label, streak)] for each kind whose ticker appears today."""
    if not operations:
        return []
    signals = operations.get("signals", {})
    hits: list[tuple[str, str, int | None]] = []
    for kind in _SIGNAL_ORDER:
        for e in signals.get(kind, []):
            if e.get("ticker") == ticker:
                hits.append((kind, _SIGNAL_LABEL.get(kind, kind), e.get("streak")))
                break
    return hits


# ── ETF holdings ──────────────────────────────────────────────────────────


def _get_etf_data(ticker: str) -> tuple[list[tuple[str, float | None]], list[dict]]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT etf_id, weight
            FROM tw.etf_holdings
            WHERE stock_id = %s
              AND trade_date = (
                  SELECT MAX(trade_date) FROM tw.etf_holdings WHERE stock_id = %s
              )
            ORDER BY weight DESC NULLS LAST
            """,
            (ticker, ticker),
        )
        holders = [
            (r["etf_id"], float(r["weight"]) if r["weight"] is not None else None)
            for r in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT etf_id, trade_date, change_type, share_diff, weight_diff
            FROM tw.etf_holdings_diff
            WHERE stock_id = %s
              AND trade_date >= %s
            ORDER BY trade_date DESC, etf_id
            LIMIT 10
            """,
            (ticker, date.today() - timedelta(days=_DIFF_LOOKBACK_DAYS)),
        )
        diffs = [dict(r) for r in cur.fetchall()]
    return holders, diffs


# ── Alerts ────────────────────────────────────────────────────────────────


def _weekdays_remaining(today: date, end_inclusive: date) -> int:
    """Count weekdays in (today, end_inclusive]. Used for future-period countdown
    (the TAIEX trading calendar has no future rows, so weekdays is the best proxy)."""
    if end_inclusive <= today:
        return 0
    cnt = 0
    d = today + timedelta(days=1)
    while d <= end_inclusive:
        if d.weekday() < 5:
            cnt += 1
        d += timedelta(days=1)
    return cnt


def _past_trading_days(today: date, n: int) -> list[date]:
    """Most recent n TWSE trading days with trade_date <= today, newest first."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
            """,
            (today, n),
        )
        return [r["trade_date"] for r in cur.fetchall()]


def _next_trading_day(today: date) -> date:
    """Next TAIEX trading day strictly after today.
    Prefer DB (tw.index_prices); fallback to weekday skip when DB hasn't
    populated future rows yet (typical — calendar lands one day at a time)."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT MIN(trade_date) AS d FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date > %s
            """,
            (today,),
        )
        r = cur.fetchone()
    if r and r.get("d"):
        return r["d"]
    d = today + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


# In-process cooldown so concurrent /score calls don't spam TWSE scrapers
_TPE_TZ = timezone(timedelta(hours=8))
_ALERT_REFRESH_COOLDOWN_S = 300
_last_alert_refresh: dict[date, datetime] = {}


def _maybe_refresh_alerts(today: date) -> None:
    """Best-effort re-scrape of yesterday + today stock_alerts.

    Scrapes BOTH days because TWSE's disposal batch often has alert_date =
    yesterday with period_start = today (announced previous evening,
    effective today). Yesterday's batch might also have been published
    AFTER daily_update ran. Cooldown prevents thrashing; failures are
    swallowed silently.
    """
    now = datetime.now(_TPE_TZ)
    last = _last_alert_refresh.get(today)
    if last and (now - last).total_seconds() < _ALERT_REFRESH_COOLDOWN_S:
        return
    _last_alert_refresh[today] = now  # set before scrape to avoid retry storms

    yesterday = today - timedelta(days=1)
    try:
        from scrapers.stock_alerts import scrape_date as _scrape_alerts
        _scrape_alerts(yesterday)
        _scrape_alerts(today)
        logger.info("stock_alerts refreshed for %s, %s", yesterday, today)
    except Exception as exc:
        logger.warning("stock_alerts refresh failed: %s", exc)


def _attention_dates_in_window(cur, ticker: str, start: date, end: date) -> set[date]:
    """Return distinct alert_dates where this stock had attention announced
    in [start, end]. Used both for counting and for detecting which day rolls
    out of the window tomorrow."""
    cur.execute(
        """
        SELECT DISTINCT alert_date FROM tw.stock_alerts
        WHERE stock_id = %s AND alert_type = 'attention'
          AND alert_date >= %s AND alert_date <= %s
        """,
        (ticker, start, end),
    )
    return {r["alert_date"] for r in cur.fetchall()}


def _get_disposal_status(
    ticker: str, freshness: Freshness | None = None, allow_refresh: bool = True
) -> str | None:
    """Tomorrow-aware disposal prediction.

    For each rule (window, threshold) ∈ {(5, 3), (10, 6), (30, 12)} compute:
      • today_count  = # attention days inside today's window
      • oldest       = the day that rolls OUT tomorrow (recent[window-1])
      • tomorrow_base = today_count - (oldest was attention day ? 1 : 0)
      • tomorrow_need = max(0, threshold - tomorrow_base)

    If today_count already >= threshold the stock will enter disposal on the
    next trading day. Otherwise tomorrow_need is "how many fresh attention
    announcements tomorrow would trigger disposal".

    `freshness` controls wording: LIVE → "預期"; CLOSED → "" (definite);
    STALE → "最近交易日" framing.
    """
    today = date.today()
    max_window = max(w for w, _, _ in _DISPOSAL_RULES)  # 30
    state = freshness.state if freshness else None

    # Try to fetch yesterday+today's stock_alerts before checking ongoing
    # disposal — TWSE often publishes disposal batches (alert_date = yesterday,
    # period_start = today) after daily_update at 9:30pm. Cooldown internally
    # makes this idempotent per session. Batch callers opt out via
    # allow_refresh=False to avoid per-stock HTTP storms.
    if allow_refresh:
        _maybe_refresh_alerts(today)

    with get_cursor(commit=False) as cur:
        # ── 1. Already in disposal? ────────────────────────────────────────
        cur.execute(
            """
            SELECT period_start, period_end, cumulative, measure
            FROM tw.stock_alerts
            WHERE stock_id = %s AND alert_type = 'disposal'
              AND period_start <= %s AND period_end >= %s
            ORDER BY period_start DESC LIMIT 1
            """,
            (ticker, today, today),
        )
        ongoing = cur.fetchone()

        recent = _past_trading_days(today, max_window)
        if not recent:
            # No TAIEX calendar — return bare ongoing if present.
            if ongoing:
                end = ongoing["period_end"]
                interval = _parse_auction_interval(ongoing.get("measure")) or "5分盤"
                next_td = _next_trading_day(today)
                if end == next_td:
                    return f"{_RED} 處置中 {interval} → 明天出處置"
                return f"{_RED} 處置中 {interval} → {end.strftime('%m/%d')} 出關"
            return None
        att_dates = _attention_dates_in_window(cur, ticker, recent[-1], today)
        # Per TWSE 作業要點 第6條第7項: attention days before + during the most
        # recent disposal don't count toward the next-disposal trigger.
        cur.execute(
            """
            SELECT MAX(period_end) AS m FROM tw.stock_alerts
            WHERE stock_id = %s AND alert_type = 'disposal'
              AND period_end IS NOT NULL AND period_end < %s
            """,
            (ticker, today),
        )
        last_disposal_end = cur.fetchone()["m"]
        if last_disposal_end:
            att_dates = {d for d in att_dates if d > last_disposal_end}
        # has_prior_disposal still used by the triggered-case message
        cur.execute(
            """
            SELECT 1 FROM tw.stock_alerts
            WHERE stock_id = %s AND alert_type = 'disposal'
              AND alert_date >= %s AND alert_date < %s
            LIMIT 1
            """,
            (ticker, today - timedelta(days=45), today),
        )
        has_prior_disposal = cur.fetchone() is not None

    # Per-rule base analysis (without intraday prediction).
    # Only 10/6 and 30/12 here — (5,5) "連續5日" and condition 1 stripped per UX simplify.
    rule_states: list[tuple[int, int, str, int, int]] = []
    for window, threshold, name in _DISPOSAL_RULES:
        win_size = min(window, len(recent))
        win_dates = set(recent[:win_size])
        today_count = len(win_dates & att_dates)
        oldest = recent[win_size - 1]
        oldest_had_att = oldest in att_dates
        tomorrow_base = today_count - (1 if oldest_had_att else 0)
        tomorrow_need = max(0, threshold - tomorrow_base)
        rule_states.append((window, threshold, name, today_count, tomorrow_need))

    # Intraday attention prediction: if today's data would trigger a §X rule,
    # conceptually today_count + 1 for every disposal rule.
    try:
        predicted = predict_today_attention(ticker, today, state)
    except Exception:
        predicted = []
    inflate = bool(predicted and today not in att_dates)

    if inflate:
        states = [
            (w, t, n, tc + 1, max(0, tn - 1))
            for w, t, n, tc, tn in rule_states
        ]
    else:
        states = rule_states

    # Compact summary: only show 5日 and 10日 progress (skip 30/12 — too noisy)
    summary_segments = [
        f"{w}日 {tc}/{t}" for w, t, _, tc, _ in states if w in (5, 10)
    ]
    summary = "｜".join(summary_segments)

    # Wording switches based on data freshness
    if state == DataState.STALE_OVERNIGHT:
        day_word = "最近交易日"
        trigger_verb = "觸發"
    elif state in (DataState.CLOSED_PENDING, DataState.CLOSED_FINAL):
        day_word = "今日"
        trigger_verb = "觸發"
    else:  # LIVE / PRE_MARKET / None
        day_word = "今日"
        trigger_verb = "預期觸發"

    predict_prefix = ""
    if predicted:
        rules_str = "/".join(rc for rc, _ in predicted[:3])
        main_detail = predicted[0][1]
        predict_prefix = f"{day_word} {rules_str} {trigger_verb}（{main_detail}）→ "

    triggered = [s for s in states if s[3] >= s[1]]
    most_pressing_need = min((s[4] for s in states), default=99)

    # ── Already in disposal: bare ongoing message, no upgrade prediction ───
    # Per TWSE 作業要點 第6條第7項: attention days BEFORE + DURING disposal
    # don't count toward the next-disposal trigger. The 5/5, 10/6, 30/12
    # counters effectively reset after exit, so rule_states here are stale
    # and any upgrade prediction would be tautological. Just show the exit
    # date.
    if ongoing:
        end = ongoing["period_end"]
        interval = _parse_auction_interval(ongoing.get("measure")) or "5分盤"
        next_td = _next_trading_day(today)
        if end == next_td:
            return f"{_RED} 處置中 {interval} → 明天出處置"
        return f"{_RED} 處置中 {interval} → {end.strftime('%m/%d')} 出關"

    if triggered:
        # Show actual threshold values (close ≥ X 元 / 量 ≥ Y 張) for the
        # §X that triggered today, so user can independently verify.
        thresh_str = ""
        if predicted:
            try:
                rule_codes = [code for code, _ in predicted[:3]]
                thresh_str = format_today_thresholds(ticker, today, rule_codes)
            except Exception:
                thresh_str = ""
        if thresh_str:
            return f"{_RED} {thresh_str} → 明日進處置"
        return f"{_RED} 明日進處置"

    # Past 10 trading days clean AND no prediction → omit
    look10 = recent[: min(10, len(recent))]
    if not predicted and not any(d in att_dates for d in look10):
        return None

    if most_pressing_need == 1:
        return f"{_ORANGE} 後天可能進處置"

    # All other tomorrow_need values (>= 2) — drop entirely per UX simplify.
    return None


# ── Formatters ────────────────────────────────────────────────────────────


def _fmt_pct(p) -> str:
    if p is None:
        return "—"
    return f"{float(p):.1f}"


def _format_side(label: str, entry: dict | None, side_color: str) -> str:
    if entry is None:
        return f"{side_color}{label}：未進排行榜"
    pct = entry.get("total_pct")
    rank = entry.get("rank")
    delta = entry.get("rank_delta")

    # Rank change marker: ↑ rank improved, ↓ dropped, 新進 first time in ranking
    if entry.get("is_new"):
        rank_marker = "  新進"
    elif isinstance(delta, (int, float)) and delta != 0:
        arrow = "↑" if delta > 0 else "↓"
        rank_marker = f"  {arrow}{abs(int(delta))}"
    else:
        rank_marker = ""

    off_tag = "（榜外）" if entry.get("off_ranking") else ""
    head = f"{side_color}{label} #{rank}{off_tag}{rank_marker}"

    # 4-point score trail, today on the left, arrow points back through history
    trail = " ← ".join(
        _fmt_pct(v)
        for v in (pct, entry.get("pct_d1"), entry.get("pct_d2"), entry.get("pct_d3"))
    )
    return f"{head}\n  分數 {trail}"


def _format_positions(positions: list[dict]) -> str | None:
    """Render open + today-exited positions, one block per row."""
    if not positions:
        return None
    blocks: list[str] = []
    for p in positions:
        side = p["_side"]
        is_exited = p["_is_exited"]
        side_zh = "多單" if side == "long" else "空單"
        color = _RED if side == "long" else _GREEN
        entry_date = p.get("entry_date")
        entry_price = p.get("entry_price")
        entry_tier = p.get("entry_tier", "")
        current = p.get("current_close")
        pnl = p.get("pnl_pct")
        bars = p.get("bars_held")

        entry_line = (
            f"  進場 {entry_date} @ {float(entry_price):.2f}"
            f"（tier={entry_tier}）"
        )

        if is_exited:
            exit_reason = p.get("exit_reason") or ""
            reason_part = f"（{exit_reason}）" if exit_reason else ""
            head = f"{_YELLOW}{side_zh} 今日已出場{reason_part}"
            exit_line = (
                f"  出場 @ {float(current):.2f}"
                f"（pnl {float(pnl):+.2f}%，持 {int(bars)} 根）"
            )
            defense = p.get("defense_price")
            lines = [head, entry_line, exit_line]
            if defense is not None:
                lines.append(f"  防守 {float(defense):.2f}")
            blocks.append("\n".join(lines))
        else:
            head = f"{color}持{side_zh}"
            cur_line = (
                f"  目前 {float(current):.2f}"
                f"（{float(pnl):+.2f}%，持 {int(bars)} 根）"
            )
            defense = p.get("defense_price")
            lines = [head, entry_line, cur_line]
            if defense is not None:
                lines.append(f"  防守 {float(defense):.2f}")
            blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _format_signals(hits: list[tuple[str, str, int | None]]) -> str | None:
    if not hits:
        return None
    parts = []
    for kind, label, streak in hits:
        color = _SIGNAL_COLOR.get(kind, "")
        tail = f"（連續 {streak} 日）" if streak else ""
        parts.append(f"{color}{label}{tail}")
    return "今日訊號：" + " / ".join(parts)


def _format_etf(holders: list[tuple[str, float | None]], diffs: list[dict]) -> str | None:
    if not holders and not diffs:
        return None
    lines: list[str] = []
    if holders:
        head = f"ETF 持有（{len(holders)} 檔）"
        # Show up to 5 with weight
        top = holders[:5]
        top_str = ", ".join(
            f"{eid} {w:.1f}%" if w is not None else eid for eid, w in top
        )
        more = f"  …其餘 {len(holders) - 5} 檔" if len(holders) > 5 else ""
        lines.append(f"{head}：{top_str}{more}")
    if diffs:
        lines.append(f"近 {_DIFF_LOOKBACK_DAYS} 日 ETF 異動：")
        for r in diffs[:5]:
            ct_key = r["change_type"]
            ct = _CHANGE_TYPE_ZH.get(ct_key, ct_key)
            color = _CHANGE_TYPE_COLOR.get(ct_key, "")
            sd = r.get("share_diff")
            wd = r.get("weight_diff")
            sd_part = f" {int(sd):+,d} 股" if sd else ""
            wd_part = ""
            if wd is not None:
                wd_part = f"（權重 {float(wd):+.2f}%）"
            lines.append(f"  {r['trade_date']} {r['etf_id']} {color}{ct}{sd_part}{wd_part}")
    return "\n".join(lines)


# ── Composer ──────────────────────────────────────────────────────────────


def _build_reply(ticker: str) -> str:
    try:
        freshness = detect_state()
    except Exception:
        freshness = None

    scores = _load_json(_SCORES_JSON)
    operations = _load_json(_OPERATIONS_JSON)
    positions = _load_json(_POSITIONS_JSON)
    signal_hits = _get_signal_hits(ticker, operations)
    position_entries = _get_positions(ticker, positions)
    try:
        etf_holders, etf_diffs = _get_etf_data(ticker)
    except Exception:
        etf_holders, etf_diffs = [], []
    try:
        disposal_line = _get_disposal_status(ticker, freshness)
    except Exception:
        disposal_line = None

    # Pull display name/market from scoreboard, fall back to nothing.
    name = ""
    market = ""
    long_entry = short_entry = None
    snapshot_date = "?"
    turnover_part = ""

    if scores is not None:
        snapshot_date = scores.get("snapshot_date", "?")
        long_entry = _find_in_ranking(scores.get("long", []), ticker)
        short_entry = _find_in_ranking(scores.get("short", []), ticker)

        # Off-ranking fallback: anything not in the JSON top-300 slice
        # is still kept in tw.score_snapshot_intraday — pull it from DB.
        if long_entry is None or short_entry is None:
            try:
                db_sides = _lookup_db_sides(ticker)
            except Exception:
                db_sides = {"long": None, "short": None}
            if long_entry is None:
                long_entry = db_sides["long"]
            if short_entry is None:
                short_entry = db_sides["short"]

        any_entry = long_entry or short_entry
        if any_entry is not None:
            name = any_entry.get("name", "")
            market = any_entry.get("market", "")
            try:
                tv_rank, tv_total, last_price = _get_turnover_rank_and_price(ticker)
            except Exception:
                tv_rank = tv_total = last_price = None
            extra_lines = []
            if last_price is not None:
                extra_lines.append(f"成交價：{last_price:.2f}")
            if tv_rank is not None and tv_total is not None:
                extra_lines.append(f"成交金額排名：#{tv_rank} / {tv_total}")
            if extra_lines:
                turnover_part = "\n" + "\n".join(extra_lines)

    # Header (with freshness tag if available)
    tag_part = f"  [{freshness.tag}]" if freshness else ""
    if name or market:
        header = f"[評分] {ticker} {name}（{market}） — {snapshot_date}{tag_part}{turnover_part}"
    else:
        header = f"[評分] {ticker} — 排行榜外（{snapshot_date}）{tag_part}"

    sections = [header]

    # ScoreBoard sides interleaved with their positions: long block then short
    # block, so the reader sees each side's score + open trade together.
    long_positions = [p for p in position_entries if p["_side"] == "long"]
    short_positions = [p for p in position_entries if p["_side"] == "short"]
    if (long_entry is not None or short_entry is not None
            or long_positions or short_positions):
        long_parts = [_format_side("做多", long_entry, _RED)]
        fmt_long = _format_positions(long_positions)
        if fmt_long:
            long_parts.append(fmt_long)
        sections.append("\n\n".join(long_parts))

        short_parts = [_format_side("做空", short_entry, _GREEN)]
        fmt_short = _format_positions(short_positions)
        if fmt_short:
            short_parts.append(fmt_short)
        sections.append("\n\n".join(short_parts))

    # Optional sections
    for formatted in (
        _format_signals(signal_hits),
        _format_etf(etf_holders, etf_diffs),
        disposal_line,
    ):
        if formatted:
            sections.append(formatted)

    return "\n\n".join(sections)


# ── Batch watchlist scoring ───────────────────────────────────────────────


def _batch_fetch_sides_and_quotes(tickers: list[str]) -> dict[str, dict]:
    """One-shot DB read of (long/short ranking row + last_price) for many
    tickers. Returns {ticker: {"long": entry|None, "short": entry|None,
    "last_price": float|None, "name": str, "market": str}}.

    Built around the same "most recent (snapshot_date, snapshot_time)" anchor
    as the single-ticker path so /watch score stays consistent with /score."""
    out: dict[str, dict] = {
        t: {"long": None, "short": None, "last_price": None, "name": "", "market": ""}
        for t in tickers
    }
    if not tickers:
        return out
    with get_cursor(commit=False) as cur:
        cur.execute("""
            SELECT s.stock_id, s.side, s.rank, s.total_pct, s.turnover,
                   s.is_new, s.prev_rank, s.rank_delta,
                   s.pct_d1, s.pct_d2, s.pct_d3,
                   st.name, st.market
            FROM tw.score_snapshot_intraday s
            JOIN tw.stocks st ON st.stock_id = s.stock_id
            WHERE s.stock_id = ANY(%s)
              AND (s.snapshot_date, s.snapshot_time) = (
                  SELECT snapshot_date, snapshot_time FROM tw.score_snapshot_intraday
                  ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
              )
        """, (list(tickers),))
        for r in cur.fetchall():
            sid = r["stock_id"]
            entry = {
                "rank": r["rank"],
                "ticker": sid,
                "name": r["name"],
                "market": r["market"],
                "total_pct": float(r["total_pct"]),
                "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
                "is_new": r["is_new"],
                "prev_rank": r["prev_rank"],
                "rank_delta": r["rank_delta"],
                "pct_d1": float(r["pct_d1"]) if r["pct_d1"] is not None else None,
                "pct_d2": float(r["pct_d2"]) if r["pct_d2"] is not None else None,
                "pct_d3": float(r["pct_d3"]) if r["pct_d3"] is not None else None,
                "off_ranking": True,
            }
            out[sid][r["side"]] = entry
            out[sid]["name"] = r["name"]
            out[sid]["market"] = r["market"]

        # Turnover rank within the snapshot
        cur.execute("""
            SELECT stock_id, rk, total FROM (
                SELECT stock_id,
                       RANK() OVER (ORDER BY turnover DESC NULLS LAST) AS rk,
                       COUNT(*) OVER () AS total
                FROM tw.score_snapshot_intraday
                WHERE (snapshot_date, snapshot_time) = (
                    SELECT snapshot_date, snapshot_time FROM tw.score_snapshot_intraday
                    ORDER BY snapshot_date DESC, snapshot_time DESC LIMIT 1
                ) AND side = 'long'
            ) x WHERE stock_id = ANY(%s)
        """, (list(tickers),))
        for r in cur.fetchall():
            out[r["stock_id"]]["tv_rank"] = int(r["rk"])
            out[r["stock_id"]]["tv_total"] = int(r["total"])

        # Latest intraday quote per ticker (DISTINCT ON keeps newest row per stock)
        cur.execute("""
            SELECT DISTINCT ON (stock_id) stock_id, last_price
            FROM tw.intraday_quotes
            WHERE stock_id = ANY(%s)
            ORDER BY stock_id, updated_at DESC
        """, (list(tickers),))
        for r in cur.fetchall():
            if r["last_price"] is not None:
                out[r["stock_id"]]["last_price"] = float(r["last_price"])
    return out


def _watchlist_sort_key(ticker: str, side_pos: dict[str, dict | None]) -> tuple:
    """Sort positions-first (pnl ascending so worst first), then alphabetical.

    Group buckets: (0) active position, (1) exited today, (2) no position."""
    long_p = side_pos.get("long")
    short_p = side_pos.get("short")
    actives = [p for p in (long_p, short_p) if p and not p["_is_exited"]]
    exited = [p for p in (long_p, short_p) if p and p["_is_exited"]]
    if actives:
        worst_pnl = min(float(p["pnl_pct"]) for p in actives)
        return (0, worst_pnl, ticker)
    if exited:
        worst_pnl = min(float(p["pnl_pct"]) for p in exited)
        return (1, worst_pnl, ticker)
    return (2, 0.0, ticker)


def _format_one_line(ticker: str, ctx: dict, side_pos: dict[str, dict | None]) -> str:
    """One-line summary per stock. Segments separated by ' ｜ ';
    leading emoji indicates position state."""
    long_e = ctx.get("long")
    short_e = ctx.get("short")
    name = ctx.get("name") or ticker
    last_price = ctx.get("last_price")
    tv_rank = ctx.get("tv_rank")
    tv_total = ctx.get("tv_total")

    # Position segment (leading emoji + pnl + defense)
    long_p = side_pos.get("long")
    short_p = side_pos.get("short")
    pos_segments = []
    head_emoji = "⚪"
    for p, side_zh in ((long_p, "多"), (short_p, "空")):
        if p is None:
            continue
        pnl = float(p["pnl_pct"])
        if p["_is_exited"]:
            head_emoji = _YELLOW
            pos_segments.append(f"{side_zh}出場 {pnl:+.2f}%")
        else:
            head_emoji = _RED if p["_side"] == "long" else _GREEN
            defense = p.get("defense_price")
            def_part = f" 防{float(defense):.2f}" if defense is not None else ""
            pos_segments.append(f"持{side_zh} {pnl:+.2f}%{def_part}")
    pos_str = " ".join(pos_segments) if pos_segments else "—"

    # Score ranks
    long_rank = long_e["rank"] if long_e else "—"
    short_rank = short_e["rank"] if short_e else "—"
    score_str = f"多#{long_rank} 空#{short_rank}"

    # Price + turnover rank
    price_str = f"{last_price:.2f}" if last_price is not None else "—"
    tv_str = f"排#{tv_rank}" if tv_rank else ""
    price_seg = f"{price_str} {tv_str}".strip()

    return f"{head_emoji} {ticker} {name}  ｜{pos_str}  ｜{score_str}  ｜{price_seg}"


def build_watchlist_summary(tickers: list[str]) -> str:
    """Compose the /watch score reply for the given tickers."""
    if not tickers:
        return "[追蹤清單評分] 清單為空"

    try:
        freshness = detect_state()
    except Exception:
        freshness = None
    tag_part = f"  [{freshness.tag}]" if freshness else ""

    scores = _load_json(_SCORES_JSON)
    positions = _load_json(_POSITIONS_JSON)
    snapshot_date = scores.get("snapshot_date", "?") if scores else "?"

    try:
        ctx_map = _batch_fetch_sides_and_quotes(tickers)
    except Exception:
        ctx_map = {t: {"long": None, "short": None, "last_price": None,
                       "name": "", "market": ""} for t in tickers}

    # Layer JSON top-300 entries over DB rows so in-rank tickers lose the
    # off_ranking flag and pick up the cleaner JSON-shaped dict.
    if scores is not None:
        for side in ("long", "short"):
            for e in scores.get(side, []):
                t = e.get("ticker")
                if t in ctx_map:
                    ctx_map[t][side] = {**e, "off_ranking": False}
                    ctx_map[t]["name"] = e.get("name", "") or ctx_map[t]["name"]
                    ctx_map[t]["market"] = e.get("market", "") or ctx_map[t]["market"]

    # Position lookup per ticker, keyed by side
    pos_map: dict[str, dict[str, dict | None]] = {}
    for t in tickers:
        entries = _get_positions(t, positions)
        pos_map[t] = {"long": None, "short": None}
        for p in entries:
            pos_map[t][p["_side"]] = p

    ordered = sorted(tickers, key=lambda t: _watchlist_sort_key(t, pos_map[t]))

    lines = [f"[追蹤清單評分] {len(tickers)} 檔 — {snapshot_date}{tag_part}"]
    lines.append("")
    for t in ordered:
        lines.append(_format_one_line(t, ctx_map[t], pos_map[t]))
    return "\n".join(lines)


@restricted
async def cmd_score(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        await update.message.reply_text(
            "用法：/score <股號>\n例：/score 2330"
        )
        return

    ticker = args[0].strip()
    await update.message.reply_text(_build_reply(ticker))


def register(application) -> None:
    application.add_handler(CommandHandler("score", cmd_score))
