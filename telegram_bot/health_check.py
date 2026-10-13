"""Data-freshness / heartbeat monitor for the invest pipeline.

Catches the *silent* failures the existing per-scraper Telegram alerts miss:

  daily    — verify the most recent trading day's daily pipeline is fully
             consistent across the snapshot tables. Healthy -> a silent OK
             ping (dead-man's switch); stale -> an audible alert naming the
             lagging tables. Run pre-open (~08:30 weekday): by then the
             previous evening's daily_update must have finished, so this is
             insensitive to whatever time daily_update actually runs.

  intraday — liveness probe for the intraday daemons. NSSM restarts them
             on a crash but not on a hang; this alerts when
             tw.intraday_quotes (sweeper) or tw.score_snapshot_intraday
             (snapshot daemon) stops advancing mid-session. Run every
             ~10 min during the session (self-gates on in_session()).

Messages are composed in Python (no CMD CP950 mangling) and sent via
notify.send_sync, which is fire-and-forget and does not need the bot daemon.

CLI:
    python -m telegram_bot.health_check daily
    python -m telegram_bot.health_check intraday
    python -m telegram_bot.health_check --self-test
"""

from __future__ import annotations

import ast
import os
import re
import sys
from collections import deque
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from db.connection import get_cursor
from intraday.session import TPE_TZ, in_session, in_snapshot_session, now_tpe
from telegram_bot.notify import send_sync

_TAG = "健康檢查"

# (label, table, date column) checked by the daily probe. Every table here
# must have advanced to the latest trading day (per tw.index_prices/TAIEX)
# once the previous evening's daily_update pipeline has finished. Whether
# TAIEX itself is current is a separate question, answered by _calendar_gap.
_DAILY_TABLES = [
    ("日線收盤", "tw.daily_prices", "trade_date"),
    ("市場廣度", "tw.market_breadth", "trade_date"),
    ("評分快照", "tw.score_snapshot", "snapshot_date"),
]

# Sources that update on their own rhythm rather than every trading day, so the
# "must equal the latest trading day" rule above would cry wolf. Each carries the
# longest gap that is still normal for it:
#   - the conference index refreshes daily inside daily_update
#   - the annual-report sweep re-checks unfiled companies weekly out of season
#   - affiliations come from quarterly financials and are refreshed by hand today,
#     so only a gap wide enough to mean "nobody ran it for two quarters" is worth
#     reporting
_PERIODIC_TABLES = [
    ("法說會索引", "tw.investor_conferences", "updated_at", 3),
    ("年報產品別營收", "tw.product_revenue_status", "updated_at", 9),
    ("轉投資關係", "tw.company_affiliations", "updated_at", 200),
]

# Periodic checks whose source is switched off on purpose. While a pause is in
# force the check is skipped and nothing is said about it; from the day after
# `expires` it runs again and its line says the pause ran out. `expires` is
# mandatory so a pause cannot outlive the decision behind it. It is a date to
# rule again by -- the practice is a review 30 days out -- not something
# derived from the source's own schedule. To extend a pause, edit `expires`
# here: the commit is the record. `check` must be a label in _PERIODIC_TABLES,
# one entry per check.
_KNOWN_PAUSES = [
    {
        "check": "年報產品別營收",
        "reason": "InvestAnnualReportSweep is disabled: it runs the working-tree "
                  "parser against the production database while that parser is "
                  "being repaired",
        "started": date(2026, 9, 14),
        "expires": date(2026, 11, 6),
        "resume_condition": "re-enabling the task is ruled on separately, after "
                            "the write boundary of the 447-company re-judgement "
                            "is complete",
    },
]


# Written by the daily pipeline but legitimately empty on a quiet day, so
# 'did not advance to the latest trading day' is not evidence of failure.
# tw.signal_snapshot holds only the signals that actually fired, and
# 2026-09-17 produced none: the must-advance rule above called that a
# pipeline failure, as it had on most days that fortnight. A daily probe
# that cries wolf is a probe nobody reads, which is how the real outage on
# 2026-09-18 went unnoticed. Liveness for that step is already covered by
# tw.score_snapshot -- analysis/daily_snapshot.run() writes both from the
# same results list, so score advancing proves the signal evaluation ran.
# The tolerance here only has to catch _save_signal_fires breaking on its
# own: fires have never been absent two sessions running, so a gap this
# wide is a real anomaly rather than a quiet market.
_SPARSE_DAILY_TABLES = [
    ("訊號快照", "tw.signal_snapshot", "snapshot_date", 5),
]

# Database backup freshness. Looks at the archives themselves, never at the
# scheduled task or at backup_monthly.py: on 2026-07-02 that script was lost
# and the task kept its slot for three months with nothing behind it. A
# monitor that imports the thing it watches goes down with it, so the folder
# and the file-name rule are restated here; tests/test_health_check_backup.py
# pins them to what the script actually writes.
#
# Only freshness is judged. Whether an archive can be restored is established
# when it is written (backup_monthly.py reads it back) and by a real restore
# (ops/RESTORE.md), not by this probe.
_BACKUP_DIR = Path(r"D:\invest_backup")
_BACKUP_NAME = re.compile(r"^invest_(\d{8})\.dump$")
# The monthly task is scheduled for the 1st at 03:00.
_BACKUP_DUE_TIME = dtime(3, 0)
# Operational grace period: how late the monthly dump may be before it counts
# as missing. A policy value, not an SLA derived from how long a backup takes.
# This probe runs at 08:30, so a dump missed on the 1st is first reported
# then, not at 05:00.
_BACKUP_GRACE = timedelta(hours=2)

# Column groups of tw.daily_prices that each come from one scraper leg, checked
# per market. MAX(trade_date) advancing says nothing about these: when one leg
# dies the rows are still written by the other legs, so the day looks complete
# while a whole column is NULL for one market (TWSE institutional 2026-07-02,
# SBL 2026-06-10, price limits 2026-06-30, after-hours 2026-05-12 all failed
# this way and went unnoticed for weeks).
_COLUMN_GROUPS = [
    ("法人", "foreign_net"),
    ("融資券", "margin_balance"),
    ("借券", "sbl_balance"),
    ("當沖", "dt_volume"),
    ("零股", "ol_price"),
    ("盤後", "ah_price"),
    ("漲跌停", "limit_up"),
    ("參考價", "ref_price"),
]

# Fraction of the trailing-median coverage below which a group counts as dead.
# Calibrated over 2024-2026 (628 trading days, 28 firings): 25 are unambiguous
# leg deaths -- 0 or 1 row against a median in the hundreds, with nothing in
# between -- so the exact ratio barely matters for those. The remaining 3 are
# real market behaviour: after-hours participation collapses on a limit-down
# day (2025-04-07) and drifts near the line when the session thins out. That
# is roughly one false alarm a year, accepted in exchange for catching a dead
# leg the day it happens.
_COLUMN_MIN_RATIO = 0.7
_COLUMN_BASELINE_DAYS = 20

# A capital reduction, split or par-value change halts a stock for several days
# and it resumes at a price on a different scale. tw.daily_prices stores raw
# closes, so the break stays in the series: 5904 went 720.00 -> 79.20 on
# 2026-08-10 and then traded at 0.14x its own 20-day average, which is enough
# to fire entry signals off nothing. Detecting the halt pattern needs no
# external feed.
_HALT_MIN_DAYS = 3
_HALT_MIN_JUMP = 0.15
_HALT_LOOKBACK_DAYS = 5   # trading days after resumption to keep reporting

# The signal factory already blanks out most of these breaks, and a break it
# blanks out needs nobody's attention. These are its two thresholds, BREAK_JUMP
# and SEAM_MIN_STEP in signal_backtest/factories/_conditions.py, copied rather
# than imported so this probe does not pull the factory's stack in. A test
# keeps both pairs equal.
_QUIET_JUMP = 0.40
_QUIET_SEAM_STEP = 0.10

# Quarterly statements. No updated_at age works here: the normal gap between
# quarters runs up to ~137 days (Q3 to the annual report), and a daily sweep
# that tops up one late filer refreshes updated_at anyway. Instead ask whether
# the latest quarter whose filing deadline has passed is actually in the
# tables. Deadlines and grace are copied from scrapers/financials.py
# (FILING_DEADLINES, FILING_GRACE_DAYS) rather than imported, so the monitor
# does not go down with the scraper; tests/test_health_check_financials.py
# keeps them equal. 2026Q2 was missing for two months before anyone noticed.
_FILING_DEADLINES = {1: (5, 15), 2: (8, 14), 3: (11, 14), 4: (3, 31)}
_FILING_GRACE_DAYS = 7
# A due quarter covering fewer companies than this share of the previous
# quarter counts as missing. A policy value; late filers and the industries
# the scraper does not map keep coverage a little under 100%.
_FILING_MIN_RATIO = 0.9
_FINANCIAL_TABLES = [
    ("損益表", "tw.income_statements"),
    ("資產負債表", "tw.balance_sheets"),
    ("現金流量表", "tw.cash_flows"),
]

# Intraday: alert when tw.intraday_quotes has not advanced for this long.
_INTRADAY_STALE_MINUTES = 5

# Snapshot daemon runs back-to-back passes (roughly one per minute); allow a
# generous margin for slow passes before calling it hung.
_SNAPSHOT_STALE_MINUTES = 15

_LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs",
)

# Sentinel files remembering "already alerted today" so the every-10-min
# intraday probe fires at most one alert per trading day per component.
_SENTINEL = os.path.join(_LOGS_DIR, ".health_intraday_alerted")
_SENTINEL_SNAPSHOT = os.path.join(_LOGS_DIR, ".health_intraday_snapshot_alerted")


def _scalar(sql: str, params: tuple = ()):
    """Run a read-only single-column query and return its value (or None)."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        return next(iter(row.values()))


def _latest_trading_day():
    return _scalar(
        "SELECT MAX(trade_date) AS d FROM tw.index_prices WHERE index_id = 'TAIEX'"
    )


def _trading_days_since(d: date, latest: date) -> int:
    """Sessions between d and latest, counted off the TAIEX calendar."""
    return _scalar(
        """
        SELECT count(*) AS n FROM tw.index_prices
        WHERE index_id = 'TAIEX' AND trade_date > %s AND trade_date <= %s
        """,
        (d, latest),
    ) or 0


def _holiday_rows(year: int) -> dict[date, bool]:
    """{holiday_date: is_closed} for one year of tw.market_holidays."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT holiday_date, is_closed FROM tw.market_holidays "
            "WHERE holiday_date BETWEEN %s AND %s",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r["holiday_date"]: r["is_closed"] for r in cur.fetchall()}


def _is_scheduled_closure(rows: dict[date, bool], d: date) -> bool | None:
    """True when the calendar closes the date, False when it covers the year
    and does not, None when it has nothing for that year.

    Same rule as daily_update._is_scheduled_closure, copied rather than
    imported; tests/test_health_check_calendar.py keeps the two equal."""
    if not rows:
        return None
    return rows.get(d, False)


# How many days before today the calendar check covers. Comparing only the
# newest day would drop a lost day from the report the morning after any later
# night succeeds; if the probe itself did not run on those mornings (machine
# off over a weekend), the day would never be reported at all. The cost is
# that an unscheduled closure (typhoon) is reported every morning it stays in
# the window, unless a row marking it closed is added to tw.market_holidays.
_CALENDAR_LOOKBACK_DAYS = 7

# How far back to look for a date the calendar closes although it traded.
_CALENDAR_CONTRADICTION_DAYS = 30


def _open_weekdays(today: date) -> tuple[list[date], list[int]]:
    """(weekdays in the lookback window that the calendar leaves open, years
    in the window the calendar has no rows for).

    The window ends yesterday: the daily probe runs in the morning, ahead of
    the evening pipeline that fetches today. A year without a calendar counts
    as having no closures, so the comparison keeps running while the calendar
    is missing instead of going quiet behind the "no calendar" line."""
    by_year: dict[int, dict[date, bool]] = {}
    days, missing = [], []
    for back in range(_CALENDAR_LOOKBACK_DAYS, 0, -1):
        d = today - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        if d.year not in by_year:
            by_year[d.year] = _holiday_rows(d.year)
            if not by_year[d.year]:
                missing.append(d.year)
        if not _is_scheduled_closure(by_year[d.year], d):
            days.append(d)
    return days, missing


def _taiex_days(first: date, last: date) -> set[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT trade_date FROM tw.index_prices "
            "WHERE index_id = 'TAIEX' AND trade_date BETWEEN %s AND %s",
            (first, last),
        )
        return {r["trade_date"] for r in cur.fetchall()}


def _closed_days_that_traded(today: date) -> list[date]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT h.holiday_date AS d
            FROM tw.market_holidays h
            JOIN tw.index_prices p
              ON p.trade_date = h.holiday_date AND p.index_id = 'TAIEX'
            WHERE h.is_closed AND h.holiday_date >= %s
            ORDER BY 1
            """,
            (today - timedelta(days=_CALENDAR_CONTRADICTION_DAYS),),
        )
        return [r["d"] for r in cur.fetchall()]


def _calendar_gap(today: date, latest: date) -> tuple[list[str], date]:
    """(problem lines, the day the data should have reached).

    latest is MAX(TAIEX), which is also what every other check measures
    against: when the index itself was not fetched, all tables stop on the
    same day and agree with each other. The exchange calendar is the only
    outside reference, and on a night the whole machine is offline — when the
    pipeline's own alert cannot be sent either — this is the line that reports
    the lost day.

    A failure of the check itself is reported as a problem, like the backup
    section: a monitor that breaks silently reads like a healthy system.
    """
    try:
        want = latest
        days, missing_years = _open_weekdays(today)
        lines = [f"休市日曆沒有 {y} 年資料（該年度只能排除週末）" for y in missing_years]
        have = _taiex_days(days[0], days[-1]) if days else set()
        lost = [d for d in days if d not in have]
        for d in lost:
            lines.append(f"大盤指數缺 {d:%m-%d}，休市日曆排定開市"
                         f"（颱風等臨時休市可忽略）")
        if lost and lost[-1] > latest:
            want = lost[-1]
        for d in _closed_days_that_traded(today):
            lines.append(f"休市日曆把 {d:%m-%d} 標為休市，但當天有大盤指數"
                         f"（日曆解析規則可能失效）")
        return lines, want
    except Exception as exc:
        return [f"交易日曆檢查本身失敗：{type(exc).__name__}: {exc}"], latest


# First night run by an exe that writes tw.daily_update_runs. Earlier nights
# have no row and must not be reported as missing. A constant rather than the
# table's earliest row: backfilling an old date would pull that back, and an
# empty table would switch the check off altogether.
_RUN_RECORDS_SINCE = date(2026, 10, 14)

# The pipeline takes about 20 minutes and the scheduler kills it at 60.
_RUN_STUCK_AFTER = timedelta(hours=2)

# daily_update's label for the export step (kept equal by
# tests/test_health_check_runs.py).
_EXPORT_STEP = "前端匯出部署"


def _run_rows(first: date) -> list[dict]:
    """Run records for trade dates from `first` on, oldest run first."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, started_at, outcome, summary_sent, steps
            FROM tw.daily_update_runs
            WHERE trade_date >= %s
            ORDER BY started_at, id
            """,
            (first,),
        )
        return [dict(r) for r in cur.fetchall()]


def _nightly_run_problems(today: date, now: datetime | None = None) -> list[str]:
    """One line per recent trading day whose nightly run needs attention.

    Reads what daily_update recorded about itself, which is the only trace of
    a failure on a night its Telegram summary could not be sent, and the only
    way to tell a run that never started from one that died.

    The newest run of a date decides, so re-running a failed date clears it.
    A core failure is always reported; a partial night only when its summary
    did not arrive. An aborted night is left to _calendar_gap, which reports
    the missing index row behind it.

    Not covered: a manual run in the afternoon of the same day counts as that
    date's run. If it gets past the gate and the scheduled run then never
    starts, the night reads as done; the tables it left short show up in the
    other checks, not here.
    """
    try:
        now = now or datetime.now(timezone.utc)
        days = [d for d in _open_weekdays(today)[0] if d >= _RUN_RECORDS_SINCE]
        if not days:
            return []
        rows = _run_rows(days[0])
        newest: dict[date, dict] = {}
        for row in rows:
            # A run started before its date had come (a manual run for a
            # future date ends at the gate) says nothing about that night.
            if row["started_at"].astimezone(TPE_TZ).date() >= row["trade_date"]:
                newest[row["trade_date"]] = row

        lines = []
        for d in days:
            row = newest.get(d)
            if row is None:
                lines.append(f"{d:%m-%d} 盤後流程沒有執行紀錄")
                continue
            outcome = row["outcome"]
            if outcome == "running":
                if now - row["started_at"] > _RUN_STUCK_AFTER:
                    began = row["started_at"].astimezone(TPE_TZ)
                    lines.append(f"{d:%m-%d} 盤後流程沒有跑完（{began:%H:%M} 開始）")
                continue
            bad = [s for s in row["steps"] or []
                   if s["status"] in ("failed", "blocked")]
            if outcome == "failed":
                # The export publishes everything current, so a later run
                # whose export went through has already made up for this one.
                # What is left of the night is then its non-core failures.
                core_bad = [s["label"] for s in bad if s.get("core")]
                if core_bad == [_EXPORT_STEP] and any(
                    r["started_at"] > row["started_at"]
                    and any(s["label"] == _EXPORT_STEP and s["status"] == "ok"
                            for s in r["steps"] or [])
                    for r in rows
                ):
                    bad = [s for s in bad if s["label"] != _EXPORT_STEP]
                    outcome = "partial" if bad else "success"
            named = "、".join(s["label"] for s in bad) or "紀錄裡沒有步驟"
            if outcome == "failed":
                lines.append(f"{d:%m-%d} 盤後流程失敗：{named}")
            elif outcome == "partial" and row["summary_sent"] is not True:
                lines.append(f"{d:%m-%d} 盤後流程有步驟失敗、摘要未送達：{named}")
        return lines
    except Exception as exc:
        return [f"盤後執行紀錄檢查本身失敗：{type(exc).__name__}: {exc}"]


def _dead_column_groups(latest: date) -> list[str]:
    """Report column groups whose coverage collapsed on the latest trading day.

    Compares each (market, group) against its own median over the preceding
    _COLUMN_BASELINE_DAYS trading days, so a group that is legitimately sparse
    (after-hours) is judged against its own level rather than a global one.
    Groups whose baseline is 0 are skipped: they predate the data (TWSE
    intraday odd-lot starts 2020-10-26) or genuinely carry nothing.
    """
    counts = ", ".join(
        f"count(p.{col}) AS c_{col}" for _, col in _COLUMN_GROUPS
    )
    with get_cursor(commit=False) as cur:
        cur.execute(
            f"""
            SELECT p.trade_date AS d, s.market AS m, {counts}
            FROM tw.daily_prices p JOIN tw.stocks s USING (stock_id)
            WHERE s.market IN ('TWSE', 'TPEx')
              AND p.trade_date > %s - INTERVAL '60 days'
              AND p.trade_date <= %s
            GROUP BY 1, 2
            ORDER BY 1
            """,
            (latest, latest),
        )
        rows = cur.fetchall()

    dead = []
    for market in ("TWSE", "TPEx"):
        series = [r for r in rows if r["m"] == market]
        today = [r for r in series if r["d"] == latest]
        if not today:
            dead.append(f"{market} 日線當日無資料")
            continue
        baseline = [r for r in series if r["d"] < latest][-_COLUMN_BASELINE_DAYS:]
        if not baseline:
            continue
        for label, col in _COLUMN_GROUPS:
            prior = sorted(r[f"c_{col}"] for r in baseline)
            median = prior[len(prior) // 2]
            actual = today[0][f"c_{col}"]
            if median > 0 and actual < median * _COLUMN_MIN_RATIO:
                dead.append(
                    f"{market} {label} 僅 {actual} 檔有值（近{len(prior)}日中位 {median}）"
                )
    return dead


# ── Built artifacts vs their sources ───────────────────────────────────────
# A committed change only reaches production once the exe carrying it is
# rebuilt, and nothing enforces that: the sweeper ran code from 2026-06-15
# for five weeks because no script covered its spec and nobody remembered.
# Comparing an exe against every module it actually bundles catches that
# without relying on anyone's memory.

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _spec_contents(spec: Path) -> tuple[list[str], str | None]:
    """(modules the exe bundles as roots, exe name) parsed from a .spec.

    hiddenimports matter as much as the entry script: daily_update reaches its
    scrapers through importlib, so a static walk from the entry alone would
    miss every one of them.
    """
    try:
        tree = ast.parse(spec.read_text(encoding="utf-8"))
    except Exception:
        return [], None

    roots: list[str] = []
    name: str | None = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id == "Analysis":
            if node.args and isinstance(node.args[0], (ast.List, ast.Tuple)):
                for el in node.args[0].elts:
                    if isinstance(el, ast.Constant) and isinstance(el.value, str):
                        roots.append(Path(el.value).stem)
            for kw in node.keywords:
                if kw.arg == "hiddenimports" and isinstance(kw.value, (ast.List, ast.Tuple)):
                    roots += [e.value for e in kw.value.elts
                              if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        elif node.func.id == "EXE":
            for kw in node.keywords:
                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    name = kw.value.value
    return roots, name


def _module_file(dotted: str) -> Path | None:
    p = _REPO_ROOT / (dotted.replace(".", "/") + ".py")
    if p.exists():
        return p
    p = _REPO_ROOT / dotted.replace(".", "/") / "__init__.py"
    return p if p.exists() else None


def _module_imports(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            out.add(node.module)
    return out


def _bundled_sources(roots: list[str]) -> list[Path]:
    """Every repo-local .py an exe built from these roots would contain."""
    seen: set[str] = set()
    files: list[Path] = []
    queue = deque(roots)
    while queue:
        dotted = queue.popleft()
        if dotted in seen:
            continue
        seen.add(dotted)
        path = _module_file(dotted)
        if path is None:            # stdlib or third party
            continue
        files.append(path)
        queue.extend(m for m in _module_imports(path) if m not in seen)
    return files


def _maintained_specs() -> list[Path]:
    """Specs some rebuild script actually builds.

    Anything else in dist/ is a manual tool nobody keeps current --
    historical_update.exe has been four months old for four months. Alerting
    on those would put a permanent entry in the daily message, which would
    destroy the point of the healthy path being a silent ping.
    """
    referenced = set()
    for bat in _REPO_ROOT.glob("*.bat"):
        try:
            text = bat.read_text(encoding="ascii", errors="ignore")
        except OSError:
            continue
        for spec in _REPO_ROOT.glob("*.spec"):
            if spec.name in text:
                referenced.add(spec)
    return sorted(referenced)


def _stale_exes() -> list[str]:
    problems = []
    for spec in _maintained_specs():
        roots, name = _spec_contents(spec)
        if not roots or not name:
            continue
        exe = _REPO_ROOT / "dist" / f"{name}.exe"
        if not exe.exists():        # never deployed; nothing to be stale
            continue
        built = exe.stat().st_mtime
        newer = [p for p in _bundled_sources(roots) if p.stat().st_mtime > built]
        if newer:
            rel = sorted(str(p.relative_to(_REPO_ROOT)).replace("\\", "/") for p in newer)
            shown = ", ".join(rel[:4]) + (f" 等 {len(rel)} 檔" if len(rel) > 4 else "")
            when = datetime.fromtimestamp(built).strftime("%m-%d %H:%M")
            problems.append(f"{name}.exe 建於 {when}，落後於 {shown}")
    return problems


def _corporate_actions(latest: date) -> list[str]:
    """Resumptions at a new price scale that the signal factory does not blank out.

    Looks at the factory's universe (active listed common stocks) and applies
    the two tests of its post_break_blackout: a step beyond _QUIET_JUMP, or a
    reduction recorded in tw.capital_changes on the resumption day with a step
    beyond _QUIET_SEAM_STEP. A break that passes either is already handled and
    is left out. What remains is a break the factory will read as a real
    move, and the note says why it was not covered.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date AS d FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 40
            """,
            (latest,),
        )
        cal = [r["d"] for r in cur.fetchall()][::-1]
        if len(cal) < _HALT_MIN_DAYS + 2:
            return []
        cur.execute(
            """
            SELECT p.stock_id, p.trade_date AS d, p.close_price, s.name
            FROM tw.daily_prices p JOIN tw.stocks s USING (stock_id)
            WHERE p.trade_date >= %s
              AND s.is_active = TRUE
              AND s.security_type = 'STOCK'
              AND s.market IN ('TWSE', 'TPEx')
            ORDER BY p.stock_id, p.trade_date
            """,
            (cal[0],),
        )
        rows = cur.fetchall()
        cur.execute(
            """
            SELECT stock_id, effective_date,
                   CASE WHEN ratio > 0 THEN abs(1 / ratio - 1) END AS step
            FROM tw.capital_changes
            WHERE event_type = 'REDUCTION' AND effective_date >= %s
            ORDER BY stock_id, effective_date
            """,
            (cal[0],),
        )
        recorded: dict[str, dict] = {}
        for r in cur.fetchall():
            recorded.setdefault(r["stock_id"], {})[r["effective_date"]] = float(r["step"] or 0)

    traded: dict[str, dict] = {}
    names: dict[str, str] = {}
    for r in rows:
        if r["close_price"] is not None:
            traded.setdefault(r["stock_id"], {})[r["d"]] = float(r["close_price"])
        names[r["stock_id"]] = r["name"] or ""

    cutoff = cal[-_HALT_LOOKBACK_DAYS] if len(cal) >= _HALT_LOOKBACK_DAYS else cal[0]
    found = []
    for sid, prices in traded.items():
        run = 0
        for day in cal:
            if day not in prices:
                run += 1
                continue
            if run >= _HALT_MIN_DAYS and day >= cutoff:
                before = [prices[x] for x in cal if x in prices and x < day]
                if before and before[-1] > 0:
                    jump = prices[day] / before[-1] - 1
                    seams = recorded.get(sid, {})
                    step = seams.get(day)
                    handled = abs(jump) > _QUIET_JUMP or (
                        step is not None and step > _QUIET_SEAM_STEP)
                    if abs(jump) >= _HALT_MIN_JUMP and not handled:
                        if step is not None:
                            tag = f"減資紀錄幅度僅 {step:.1%}，未達靜默門檻"
                        elif seams:
                            dates = "、".join(f"{d:%m-%d}" for d in sorted(seams))
                            tag = f"減資紀錄生效日 {dates} 對不上復牌日"
                        else:
                            tag = "capital_changes 無紀錄"
                        found.append(
                            f"{sid} {names.get(sid, '')} {day:%m-%d} 復牌 "
                            f"{before[-1]:g} -> {prices[day]:g} ({jump:+.0%})，{tag}"
                        )
            run = 0
    return sorted(found)


def _backup_due_date(now: datetime) -> date:
    """The 1st whose monthly backup must exist by `now`.

    Derived from the monthly cycle rather than a fixed age, so the turn of the
    month does not raise a false alarm: until the 1st at 03:00 plus the grace
    period, last month's dump is still the current one.
    """
    this_first = now.date().replace(day=1)
    if now >= datetime.combine(this_first, _BACKUP_DUE_TIME) + _BACKUP_GRACE:
        return this_first
    return (this_first - timedelta(days=1)).replace(day=1)


def _backup_problems(now: datetime, backup_dir: Path | None = None) -> list[str]:
    """What is wrong with the newest completed archive, if anything.

    A completed archive is a file named invest_YYYYMMDD.dump whose date
    parses. backup_monthly.py only gives a file that name after reading it
    back, so a .partial or anything else in the folder does not count.
    """
    backup_dir = backup_dir or _BACKUP_DIR
    try:
        names = sorted(p.name for p in backup_dir.iterdir())
    except OSError as exc:
        return [f"備份目錄讀不到：{backup_dir}（{type(exc).__name__}）"]

    dumps = []
    for name in names:
        m = _BACKUP_NAME.match(name)
        if not m:
            continue
        try:
            dumps.append((datetime.strptime(m.group(1), "%Y%m%d").date(), name))
        except ValueError:
            continue
    if not dumps:
        partial = [n for n in names if n.endswith(".partial")]
        tail = f"（只有未完成的 {', '.join(partial)}）" if partial else ""
        return [f"{backup_dir} 裡沒有任何完成的備份{tail}"]

    newest_date, newest = max(dumps)
    due = _backup_due_date(now)
    if newest_date < due:
        age = (now.date() - newest_date).days
        return [f"最新一份是 {newest}（{age} 天前），{due:%m-%d} 的月備沒有產出"]
    return []


def _backup_section(now: datetime) -> str | None:
    """The backup paragraph of the daily message, or None when it is fresh.

    A failure of the check itself is reported as a problem: a monitor that
    breaks silently reads exactly like a healthy system.
    """
    try:
        problems = _backup_problems(now)
    except Exception as exc:
        problems = [f"備份檢查本身失敗：{type(exc).__name__}: {exc}"]
    if not problems:
        return None
    return "資料庫備份異常：\n- " + "\n- ".join(problems)


def _due_quarter(today: date) -> tuple[int, int]:
    """Latest (year, quarter) whose filing deadline plus grace has passed."""
    best = None
    for year in (today.year - 2, today.year - 1, today.year):
        for quarter, (m, d) in _FILING_DEADLINES.items():
            due = date(year + 1 if quarter == 4 else year, m, d)
            if due + timedelta(days=_FILING_GRACE_DAYS) <= today:
                best = max(best or (year, quarter), (year, quarter))
    return best


def _previous_quarter(year: int, quarter: int) -> tuple[int, int]:
    return (year, quarter - 1) if quarter > 1 else (year - 1, 4)


def _financial_coverage(today: date) -> list[str]:
    """One line per statement table whose due quarter is short of companies."""
    year, quarter = _due_quarter(today)
    prev = _previous_quarter(year, quarter)
    m, d = _FILING_DEADLINES[quarter]
    lines = []
    for label, table in _FINANCIAL_TABLES:
        sql = (f"SELECT COUNT(DISTINCT stock_id) AS n FROM {table} "
               f"WHERE year = %s AND quarter = %s")
        have = _scalar(sql, (year, quarter)) or 0
        base = _scalar(sql, prev) or 0
        if base and have < _FILING_MIN_RATIO * base:
            lines.append(f"季報 {label} {year}Q{quarter} 僅 {have:,} 家"
                         f"（上一季 {base:,} 家；期限 {m:02d}-{d:02d}）")
    return lines


def _pause(label: str, today: date) -> tuple[bool, str]:
    """(skip this periodic check, note for its line) on `today`."""
    for p in _KNOWN_PAUSES:
        if p["check"] != label or today < p["started"]:
            continue
        if today <= p["expires"]:
            return True, ""
        return False, f"，暫停已於 {p['expires']:%m-%d} 到期、需重新裁定"
    return False, ""


def run_daily() -> int:
    latest = _latest_trading_day()
    if latest is None:
        send_sync(f"[{_TAG}] 找不到任何交易日資料（index_prices 為空）")
        return 1

    stale = []
    for label, table, col in _DAILY_TABLES:
        d = _scalar(f"SELECT MAX({col}) AS d FROM {table}")
        if d is None or d < latest:
            seen = d.strftime("%m-%d") if d else "無"
            stale.append(f"{label} 停在 {seen}")

    for label, table, col, max_gap in _SPARSE_DAILY_TABLES:
        d = _scalar(f"SELECT MAX({col}) AS d FROM {table}")
        if d is None:
            stale.append(f"{label} 無任何資料")
            continue
        gap = _trading_days_since(d, latest)
        if gap > max_gap:
            stale.append(
                f"{label} 停在 {d:%m-%d}（已 {gap} 個交易日未更新，"
                f"容許 {max_gap}）"
            )

    today = date.today()
    for label, table, col, max_age in _PERIODIC_TABLES:
        paused, note = _pause(label, today)
        if paused:
            continue
        ts = _scalar(f"SELECT MAX({col}) AS d FROM {table}")
        if ts is None:
            stale.append(f"{label} 無任何資料{note}")
            continue
        age = (datetime.now(timezone.utc) - ts).days
        if age > max_age:
            stale.append(f"{label} 已 {age} 天未更新（容許 {max_age} 天）{note}")

    stale.extend(_dead_column_groups(latest))
    stale.extend(_financial_coverage(today))
    stale.extend(_stale_exes())
    calendar_lines, should_reach = _calendar_gap(today, latest)
    stale.extend(calendar_lines)
    stale.extend(_nightly_run_problems(today))

    actions = _corporate_actions(latest)

    want = should_reach.strftime("%m-%d")
    sections = []
    if stale:
        sections.append(
            "盤後 pipeline 異常（應更新至 {}）：\n- {}".format(
                want, "\n- ".join(stale)
            )
        )
    if actions:
        sections.append(
            "公司行為，斷點未被訊號工廠靜默涵蓋：\n- {}".format(
                "\n- ".join(actions)
            )
        )
    # Its own paragraph: an old backup is not a stale pipeline table.
    backup = _backup_section(datetime.now())
    if backup:
        sections.append(backup)
    if sections:
        send_sync(f"[{_TAG}] " + "\n\n".join(sections))
        return 1

    send_sync(f"[{_TAG}] ✓ 系統正常（資料更新至 {want}）", silent=True)
    return 0


def run_intraday() -> int:
    now = now_tpe()
    if not in_session(now):
        return 0

    today = now.date()
    rc = _check_sweeper(now, today)
    if in_snapshot_session(now):
        # The snapshot daemon stops mid-session passes at 13:30 and idles
        # until the post-close final pass, so only probe it before 13:30.
        rc = max(rc, _check_snapshot(now, today))
    return rc


def _check_sweeper(now, today: date) -> int:
    traded = _scalar("SELECT MAX(trade_date) AS d FROM tw.intraday_quotes")
    if traded != today:
        # No rows for today yet: either a holiday or the sweeper has not
        # produced its first tick. The first-tick case is a startup problem,
        # not a mid-session hang, so stay silent here.
        return 0

    last_upd = _scalar("SELECT MAX(updated_at) AS t FROM tw.intraday_quotes")
    if last_upd is None:
        return 0

    age_min = (now - last_upd).total_seconds() / 60.0
    if age_min > _INTRADAY_STALE_MINUTES:
        if not _already_alerted(today):
            send_sync(
                f"[{_TAG}] 盤中 sweeper 停擺：intraday_quotes 已 {int(age_min)} 分鐘未更新"
            )
            _mark_alerted(today)
        return 1

    # Healthy again -> re-arm the once-per-day alert.
    _clear_alerted()
    return 0


def _check_snapshot(now, today: date) -> int:
    snapped = _scalar(
        "SELECT MAX(snapshot_date) AS d FROM tw.score_snapshot_intraday"
    )
    if snapped != today:
        # No rows for today yet: holiday, or the daemon is still in its
        # h(t)-warm-up window and has not written a first pass. Both are
        # startup conditions, not a mid-session hang, so stay silent here.
        return 0

    last_pass = _scalar(
        "SELECT MAX(snapshot_time) AS t FROM tw.score_snapshot_intraday"
    )
    if last_pass is None:
        return 0

    age_min = (now - last_pass).total_seconds() / 60.0
    if age_min > _SNAPSHOT_STALE_MINUTES:
        if not _already_alerted(today, _SENTINEL_SNAPSHOT):
            send_sync(
                f"[{_TAG}] 盤中 snapshot daemon 停擺：score_snapshot_intraday "
                f"已 {int(age_min)} 分鐘未更新"
            )
            _mark_alerted(today, _SENTINEL_SNAPSHOT)
        return 1

    _clear_alerted(_SENTINEL_SNAPSHOT)
    return 0


def run_self_test() -> int:
    ok = send_sync(f"[{_TAG}] 自我測試：投遞鏈路正常")
    return 0 if ok else 1


def _already_alerted(today: date, sentinel: str = _SENTINEL) -> bool:
    try:
        with open(sentinel, encoding="ascii") as f:
            return f.read().strip() == today.isoformat()
    except OSError:
        return False


def _mark_alerted(today: date, sentinel: str = _SENTINEL) -> None:
    try:
        os.makedirs(os.path.dirname(sentinel), exist_ok=True)
        with open(sentinel, "w", encoding="ascii") as f:
            f.write(today.isoformat())
    except OSError:
        pass


def _clear_alerted(sentinel: str = _SENTINEL) -> None:
    try:
        os.remove(sentinel)
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    mode = args[0] if args else ""
    if mode == "daily":
        return run_daily()
    if mode == "intraday":
        return run_intraday()
    if mode == "--self-test":
        return run_self_test()
    print(
        "usage: python -m telegram_bot.health_check {daily|intraday|--self-test}",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
