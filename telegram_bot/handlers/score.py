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
#
# Empirically derived from the disposal_prediction_audit history:
#   第1款 (六日累積漲幅) triggers disposal when a stock hits 第1款
#   attention on 3 CONSECUTIVE trading days — not a 30-day count.
#   In the audit data: 74/76 TPs had consec≥3; 0/100 FPs did.
# Other 款項 stay audit-only until enough data lands to lock down their
# thresholds (likely also consecutive-based, just with different counts).
#
# Format: kuan_code -> consecutive_trading_days_required
_KUAN_DISPOSAL_RULES: dict[int, int] = {
    1: 3,
}
# Escalated re-disposal: a run of N CONSECUTIVE trading days of attention
# (ANY 款 pooled, counter reset after the prior disposal's period_end)
# triggers a heightened/2nd disposal ("連續五次"). Validated on the
# disposal_prediction_audit replay: +6 TP (incl. 8021 6/16 連續五次),
# 4 FP → precision 0.970 vs 1.000 baseline. Hard-tier (🔴).
_CONSEC_DISPOSAL_AGG = 5
# Soft (🟠) cumulative-count triggers: ≥need attention days within the last
# `window` trading days (TWSE 最近十個營業日六次 / 三十個營業日十二次). Noisier
# (precision ~0.89) so surfaced as a hedge, never a definitive 明日進處置.
_COUNT_DISPOSAL_RULES: list[tuple[int, int]] = [(10, 6), (30, 12)]
# Lookback window for the attention scans — wide enough for the widest rule
# (the 30個營業日十二次 count rule); the consecutive scans use only its head.
_CONSEC_LOOKBACK_TD = 30

# Maps 第一款 / 第十一款 etc. to int.
_CN_NUMERAL = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
    "十": 10, "十一": 11, "十二": 12, "十三": 13,
}
# Matches both half-width (第N款) and full-width / square-bracket variants.
_KUAN_PATTERN = re.compile(r"第(十[一二三]?|[一二三四五六七八九])款")


def _extract_kuan_codes(reason: str | None) -> set[int]:
    """Pull 「第N款」 codes out of a TWSE attention reason text.

    A single attention row may cite multiple 款 — they're all tracked
    independently."""
    if not reason:
        return set()
    return {_CN_NUMERAL[m] for m in _KUAN_PATTERN.findall(reason) if m in _CN_NUMERAL}


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


def _lookup_name_market(ticker: str) -> tuple[str, str]:
    """Lightweight name+market lookup for tickers outside the JSON top-500
    slice. We deliberately do NOT pull rank/score from DB — those rows reply
    with "名單外" instead. Returns ("", "") if the ticker is unknown."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT name, market FROM tw.stocks WHERE stock_id = %s",
            (ticker,),
        )
        r = cur.fetchone()
    if r is None:
        return "", ""
    return r["name"] or "", r["market"] or ""


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
    max_window = _CONSEC_LOOKBACK_TD
    state = freshness.state if freshness else None

    # Convertible bond → mirror its underlying stock's disposal status first;
    # only if the underlying is clean do we fall through to the CB's own
    # attention-based rules (a CB can also be disposed on its own pattern).
    cb_und = _cb_underlying(ticker)
    if cb_und is not None:
        und_status = _get_disposal_status(cb_und, freshness, allow_refresh)
        if und_status:
            return f"{und_status}（連動標的 {cb_und}）"

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
                resume = _next_trading_day(end)
                end_str = end.strftime("%m/%d")
                resume_str = resume.strftime("%m/%d")
                if end == today:
                    return f"{_GREEN} 處置中 {interval} → 今日最後一天，{resume_str} 起恢復"
                return f"{_RED} 處置中 {interval} → {end_str} 止（{resume_str} 起恢復）"
            return None

        # Pull attention rows with their reasons so we can bucket by 款項.
        # The old logic counted attention days as a single pool which over-
        # triggered "明日進處置" on stocks whose attentions came from
        # distinct 款項 (each accumulates independently).
        #
        # Note: empirically TWSE does NOT exclude attention days that
        # overlap with a prior disposal — see 6182's 5/25 case where the
        # previous disposal ended 5/25 and the new consec run started
        # exactly that day. So no "post-disposal filter" here.
        buckets = _attention_kuan_buckets(cur, ticker, recent[-1], today)
        last_disp_end = _last_disposal_end(cur, ticker, today)

        # Throttle: if TWSE just announced a disposal in the past 3 td
        # (strictly before today — today's announcement is the one
        # we're predicting), suppress this repeat. Mirrors the
        # disposal_audit empirical tuning that took precision from
        # 71.3% → 100% at the 3 td window. Rarely fires for the live
        # bot because the ongoing branch usually returns first, but
        # catches the very-short-disposal edge case (period ended,
        # alert still recent).
        recently_announced = False
        if len(recent) > 3:
            floor_td = recent[3]
            cur.execute(
                """
                SELECT 1 FROM tw.stock_alerts
                WHERE stock_id = %s AND alert_type = 'disposal'
                  AND alert_date > %s AND alert_date < %s
                LIMIT 1
                """,
                (ticker, floor_td, today),
            )
            recently_announced = cur.fetchone() is not None

    # Intraday attention prediction: if today's data would trigger a §X rule,
    # conceptually attention is added on `today` — bump the matching 款 bucket.
    try:
        predicted = predict_today_attention(ticker, today, state)
    except Exception:
        predicted = []
    inflated = {k: set(v) for k, v in buckets.items()}
    if predicted:
        for code, _ in predicted:
            # predict_today_attention returns rule codes like "§1" / "§4";
            # map to 款 number via the trailing digit.
            try:
                kuan = int("".join(ch for ch in code if ch.isdigit()))
            except ValueError:
                continue
            inflated.setdefault(kuan, set()).add(today)

    # Per-款 rule check. Only 款項 listed in _KUAN_DISPOSAL_RULES warn;
    # the rest stay audit-only. Trigger requires N CONSECUTIVE trading
    # days of 第N款 attention (TWSE 「連續 N 日」 rule).
    # `kuan=None` marks the pooled-款 escalated 連續5 rule.
    triggered: list[tuple[int | None, int, int]] = []  # (kuan, run, threshold)
    consec_runs: dict[int, int] = {}
    for kuan, consec_req in _KUAN_DISPOSAL_RULES.items():
        run = _consec_run_ending_at(inflated.get(kuan, set()), recent)
        consec_runs[kuan] = run
        if run >= consec_req and not recently_announced:
            triggered.append((kuan, run, consec_req))

    # Escalated re-disposal: pooled-款 consecutive run (counter reset after
    # the prior disposal) ≥ 5. Catches 「連續五次」 repeat offenders like 8021.
    agg_days = _aggregate_attention_days(inflated, after=last_disp_end)
    agg_run = _consec_run_ending_at(agg_days, recent)
    if agg_run >= _CONSEC_DISPOSAL_AGG and not recently_announced:
        triggered.append((None, agg_run, _CONSEC_DISPOSAL_AGG))

    # ── Already in disposal: bare ongoing message, no upgrade prediction ───
    # Per TWSE 作業要點 第6條第7項: attention days BEFORE + DURING disposal
    # don't count toward the next-disposal trigger. The 5/5, 10/6, 30/12
    # counters effectively reset after exit, so rule_states here are stale
    # and any upgrade prediction would be tautological. Show last day +
    # the trading day normal-trade resumes (period_end is inclusive).
    if ongoing:
        end = ongoing["period_end"]
        interval = _parse_auction_interval(ongoing.get("measure")) or "5分盤"
        resume = _next_trading_day(end)
        end_str = end.strftime("%m/%d")
        resume_str = resume.strftime("%m/%d")
        if end == today:
            return f"{_GREEN} 處置中 {interval} → 今日最後一天，{resume_str} 起恢復"
        return f"{_RED} 處置中 {interval} → {end_str} 止（{resume_str} 起恢復）"

    if triggered:
        # Triggered string: "第1款 連3日" for 款 rules, "連續5日（加重）" for the
        # pooled-款 escalated rule (kuan=None).
        parts = [
            f"連續{c}日（加重）" if k is None else f"第{k}款 連{c}日"
            for k, c, _ in triggered
        ]
        thresh_str = ""
        if predicted:
            try:
                rule_codes = [code for code, _ in predicted[:3]]
                thresh_str = format_today_thresholds(ticker, today, rule_codes)
            except Exception:
                thresh_str = ""
        head = thresh_str + " → " if thresh_str else ""
        return f"{_RED} {head}明日進處置（{'｜'.join(parts)}）"

    # Soft (🟠) cumulative-count hint: noisier than the consecutive rules
    # (precision ~0.89), so hedged wording rather than a definitive call.
    if not recently_announced:
        for window, need in _COUNT_DISPOSAL_RULES:
            cnt = len(agg_days & set(recent[:window]))
            if cnt >= need:
                return f"{_ORANGE} 明日恐進處置（近{window}日已{cnt}次注意）"

    # Past 10 trading days clean across all buckets → omit
    look10 = set(recent[: min(10, len(recent))])
    any_recent_att = any(
        bool(dates & look10) for dates in inflated.values()
    )
    if not any_recent_att:
        return None

    # On-the-edge: 第1款 consec run = 2 (one short of 3) → soft warning.
    if consec_runs.get(1, 0) == 2:
        return f"{_ORANGE} 第1款 連2日 — 再 1 日即進處置"

    return None


def _consec_run_ending_at(
    kuan_dates: set[date], trading_days: list[date]
) -> int:
    """Length of the consecutive trading-day attention run that ENDS on
    ``trading_days[0]`` (the most recent / as_of day). Returns 0 if as_of
    itself has no attention — TWSE only triggers disposal on the day a
    stock just hit attention and that hit extends a 3-day run.

    Weekends are skipped automatically because ``trading_days`` is sourced
    from tw.index_prices (TAIEX calendar) — so 5/29 → 6/1 counts as
    consecutive even though they straddle a weekend."""
    if not kuan_dates or not trading_days:
        return 0
    run = 0
    for d in trading_days:
        if d not in kuan_dates:
            return run
        run += 1
    return run


def _attention_kuan_buckets(
    cur, ticker: str, start: date, end: date
) -> dict[int, set[date]]:
    """For each 款 cited in the attention rows for `ticker` in [start, end],
    return the set of alert_dates that cited it. One attention day can land
    in multiple buckets (the reason text often cites several 款 in one
    announcement)."""
    cur.execute(
        """
        SELECT alert_date, reason
        FROM tw.stock_alerts
        WHERE stock_id = %s AND alert_type = 'attention'
          AND alert_date >= %s AND alert_date <= %s
        """,
        (ticker, start, end),
    )
    buckets: dict[int, set[date]] = {}
    for r in cur.fetchall():
        for k in _extract_kuan_codes(r["reason"]):
            buckets.setdefault(k, set()).add(r["alert_date"])
    return buckets


def _cb_underlying(ticker: str) -> str | None:
    """If `ticker` is a 上市/上櫃 convertible-bond code (5 digits = 4-digit
    underlying stock + 1 sequence digit, e.g. 80212 → 8021), return the
    underlying stock code, else None. A CB is auto-disposed on the exact same
    day/period as its underlying (TWSE 「轉(交)換公司債之標的證券…發布處置」), so
    we mirror the underlying's disposal status rather than tracking the CB."""
    if len(ticker) == 5 and ticker.isdigit() and ticker[0] != "0":
        return ticker[:4]
    return None


def _last_disposal_end(cur, ticker: str, before: date) -> date | None:
    """Most recent disposal period_end strictly before `before` (None if the
    stock has never been disposed). Used to reset the attention counters
    after a disposal — TWSE 作業要點 第6條第7項: attention days on/before a
    prior disposal don't count toward the next disposal trigger."""
    cur.execute(
        """
        SELECT MAX(period_end) AS e FROM tw.stock_alerts
        WHERE stock_id = %s AND alert_type = 'disposal' AND period_end < %s
        """,
        (ticker, before),
    )
    r = cur.fetchone()
    return r["e"] if r and r["e"] else None


def _aggregate_attention_days(
    buckets: dict[int, set[date]], after: date | None = None
) -> set[date]:
    """Union of attention days across all 款 buckets, dropping days on/before
    `after` (the prior disposal's period_end) so the count/consecutive
    triggers reset after each disposal."""
    days: set[date] = set()
    for s in buckets.values():
        days |= s
    if after is not None:
        days = {d for d in days if d > after}
    return days


def predict_disposal_trigger(
    cur, ticker: str, as_of: date, recent: list[date]
) -> tuple[bool, dict[int, int]]:
    """Pure-DB version of the disposal trigger check used by the audit.

    Returns ``(would_trigger, kuan_counts)`` where ``kuan_counts`` maps each
    cited 款 to the in-window count after applying the "after last disposal"
    filter. No intraday prediction inflation — audit runs at EOD when the
    actual attention for ``as_of`` is already in the DB.

    ``recent`` is the list of the last N trading days (newest first) up to
    and including ``as_of``; caller supplies it so we don't re-query inside
    the loop.

    Returns ``(False, ...)`` if the stock is already in ongoing disposal at
    ``as_of`` — TWSE won't double-enter, and neither does the live bot.
    """
    if not recent:
        return False, {}
    # Stack-suppression: TWSE doesn't re-announce disposal on a stock
    # the day after it just announced one. Two-part check:
    #   1. in_disposal: a disposal period covers as_of
    #   2. recently_announced: a disposal alert landed in the past 2
    #      trading days (strictly before as_of — today's alert is
    #      what we're predicting)
    # We suppress only when BOTH hold — stocks like 8291/6173 that
    # stack disposals every ~3 td slip through #1 alone (the period
    # still covers as_of but the alert is older).
    cur.execute(
        """
        SELECT 1 FROM tw.stock_alerts
        WHERE stock_id = %s AND alert_type = 'disposal'
          AND period_start <= %s AND period_end >= %s
        LIMIT 1
        """,
        (ticker, as_of, as_of),
    )
    in_disposal = cur.fetchone() is not None

    # Empirically tuned: 3 td hits 100% precision / 74% recall on the
    # 14-day audit backfill. 2 td drops precision to 71% (more FPs from
    # back-to-back disposals); 4 td drops recall to 57% (over-blocks
    # legitimate stacked-disposal triggers like 8291).
    THROTTLE_TD = 3
    recently_announced = False
    if in_disposal and len(recent) > THROTTLE_TD:
        floor_td = recent[THROTTLE_TD]
        cur.execute(
            """
            SELECT 1 FROM tw.stock_alerts
            WHERE stock_id = %s AND alert_type = 'disposal'
              AND alert_date > %s AND alert_date < %s
            LIMIT 1
            """,
            (ticker, floor_td, as_of),
        )
        recently_announced = cur.fetchone() is not None

    buckets = _attention_kuan_buckets(cur, ticker, recent[-1], as_of)
    # No post-disposal filter — TWSE empirically counts attention days
    # that fall on or after the prior disposal's period_end (see 6182).
    counts: dict[int, int] = {}
    triggered = False
    for kuan, consec_req in _KUAN_DISPOSAL_RULES.items():
        run = _consec_run_ending_at(buckets.get(kuan, set()), recent)
        counts[kuan] = run
        if run >= consec_req and not recently_announced:
            triggered = True
    # Escalated re-disposal: pooled-款 consecutive run (counter reset after the
    # prior disposal) ≥ 5. Mirrors the live bot's 連續5（加重）hard trigger.
    if not in_disposal:
        agg = _aggregate_attention_days(
            buckets, after=_last_disposal_end(cur, ticker, as_of)
        )
        if _consec_run_ending_at(agg, recent) >= _CONSEC_DISPOSAL_AGG:
            triggered = True
    # Convertible bond → also fires when its underlying stock enters disposal
    # (mirror), on top of the CB's own attention-based rules above.
    if not triggered:
        cb_und = _cb_underlying(ticker)
        if cb_und is not None:
            triggered = predict_disposal_trigger(cur, cb_und, as_of, recent)[0]
    # Also report consec runs for non-rule 款 so the audit can see what's
    # building up even when no rule fires.
    for kuan, dates in buckets.items():
        if kuan not in counts:
            counts[kuan] = _consec_run_ending_at(dates, recent)
    return triggered, counts


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

    off_ranking = False
    if scores is not None:
        snapshot_date = scores.get("snapshot_date", "?")
        long_entry = _find_in_ranking(scores.get("long", []), ticker)
        short_entry = _find_in_ranking(scores.get("short", []), ticker)

        # Off-ranking: not in the JSON top-500 slice on either side.
        # We still want a useful reply (signals / positions / ETF /
        # disposal), so fetch name+market only and tag the header
        # "名單外".
        if long_entry is None and short_entry is None:
            off_ranking = True
            try:
                name, market = _lookup_name_market(ticker)
            except Exception:
                name, market = "", ""
        else:
            any_entry = long_entry or short_entry
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
    if off_ranking:
        if name or market:
            header = f"[評分] {ticker} {name}（{market}） — 名單外（{snapshot_date}）{tag_part}{turnover_part}"
        else:
            header = f"[評分] {ticker} — 名單外（{snapshot_date}）{tag_part}"
    elif name or market:
        header = f"[評分] {ticker} {name}（{market}） — {snapshot_date}{tag_part}{turnover_part}"
    else:
        header = f"[評分] {ticker} — {snapshot_date}{tag_part}"

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
    """Sort by signed distance to defense, ascending: broken (negative) first,
    then thin margin, then fat margin. No-position stocks at bottom by symbol.

    Long-safe direction is current > defense (positive); short-safe is current
    < defense (positive after sign flip). Already-exited rows score negative
    and naturally float above active rows with positive cushion."""
    positions = [p for p in (side_pos.get("long"), side_pos.get("short")) if p]
    if not positions:
        return (1, 0.0, ticker)

    def _signed_dist_pct(p: dict) -> float:
        defense = p.get("defense_price")
        current = p.get("current_close")
        if defense is None or current is None or float(defense) == 0:
            return float("inf")
        raw = (float(current) - float(defense)) / float(defense) * 100.0
        return raw if p["_side"] == "long" else -raw

    return (0, min(_signed_dist_pct(p) for p in positions), ticker)


def _format_one_line(ticker: str, ctx: dict, side_pos: dict[str, dict | None]) -> str:
    """One-line summary per stock. Segments separated by ' ｜ ';
    leading emoji indicates position state."""
    long_e = ctx.get("long")
    short_e = ctx.get("short")
    name = ctx.get("name") or ticker
    last_price = ctx.get("last_price")
    tv_rank = ctx.get("tv_rank")
    tv_total = ctx.get("tv_total")

    # Position segment (leading emoji + pnl + defense). The lead emoji is
    # state-based, not side-based: 🔴 already exited, 🟡 within 5% of defense
    # (caution), 🟢 safe margin, ⚪ no position. Worst state across both
    # sides wins.
    long_p = side_pos.get("long")
    short_p = side_pos.get("short")
    pos_segments = []
    state_emoji = {0: "⚪", 1: _GREEN, 2: _YELLOW, 3: _RED}
    state_priority = 0
    for p, side_zh in ((long_p, "多"), (short_p, "空")):
        if p is None:
            continue
        defense = p.get("defense_price")
        current = p.get("current_close")
        def_part = f" 防{float(defense):.2f}" if defense is not None else ""
        if p["_is_exited"]:
            this_state = 3
            pos_segments.append(f"{side_zh}出場{def_part}")
        else:
            within_5pct = (
                defense is not None and current is not None and float(defense) != 0
                and abs(float(current) - float(defense)) / float(defense) < 0.05
            )
            this_state = 2 if within_5pct else 1
            pos_segments.append(f"持{side_zh}{def_part}")
        if this_state > state_priority:
            state_priority = this_state
    head_emoji = state_emoji[state_priority]
    pos_str = " ".join(pos_segments) if pos_segments else "—"

    # Long-side rank only — short rank elided by user preference
    # (short_pct is the mirror of long_pct so it adds no extra info).
    long_rank = long_e["rank"] if long_e else "—"
    score_str = f"多#{long_rank}"

    # Price sits next to the name; turnover rank takes the final segment.
    price_str = f"{last_price:.2f}" if last_price is not None else "—"
    name_with_price = f"{name} {price_str}"
    tv_str = f"排#{tv_rank}" if tv_rank else "—"

    return f"{head_emoji} {ticker} {name_with_price}  ｜{pos_str}  ｜{score_str}  ｜{tv_str}"


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
