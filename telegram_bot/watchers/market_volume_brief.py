"""Intraday market-volume brief — 09:10–10:30 TPE.

Answers one question repeatedly through the early session: is today's turnover
going to come in above or below recent days? Today's cumulative turnover is
divided by the h(t) curve to project a full-day figure, which is then compared
against yesterday and the 5-day average.

Push times are dense early because that is where the answer firms up. Measured
over 61 trading days: days whose full-day turnover ends up more than 15% away
from the 5-day average were already called correctly 100% of the time by 09:30
(n=22), while the 5–15% band was only 71% right at 09:30 and needed ~90 minutes
to reach 88%. Days within 5% are a coin flip all morning — hence the explicit
confidence tier in every message rather than a bare up/down verdict.

Figures come from tw.intraday_value_profile, which the sweeper fills every 5
minutes. twse_trade_value / taiex_* are the exchange's own published numbers
(via IX0001/IX0043); market_total_value is our own sum whose unit doesn't match
published turnover, so it is only ever used as a ratio.

Known mismatch: the h(t) curve is still built from market_total_value (TSE+OTC
combined) while the projection divides TWSE-only turnover by it, so any
difference in the two exchanges' intraday shape leaks into the projection.
TWSE is ~81% of the total and both exchanges trade the same session, so the
error is small next to the 5%/15% verdict bands — but once the new columns have
~21 trading days of history the curve should be rebuilt on twse_trade_value
alone and this note deleted.

Calibration: the 13:30 bucket is the last sweep before the closing auction, so
h(t) projects to a figure that leaves the auction out — 90% of the official
close on the median day over 2026-08-10..09-10 (69% on 08-31, an index-rebalance
close) — while the prev-day / 5-day baselines are official closes. Uncorrected,
every projection read ~11% low and ordinary days came out as 量縮. Projections
are scaled by the median official/13:30 ratio of the preceding 20 days, TWSE and
TPEx separately (TPEx sits nearer 0.94); leave-one-out median error at 10:30 fell
from 11.5% to 5.2%. The 5%/15% bands above were measured on same-basis ratios,
which is what the calibration restores. Rebalance-day closes still land low.

CLI (force-build, ignores trading-day + schedule gates):
    python -m telegram_bot.watchers.market_volume_brief
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, datetime, time as dtime, timedelta, timezone
from statistics import median

from db.connection import get_cursor
from intraday.estimate import get_h_curve
from telegram_bot.notify import send_sync

logger = logging.getLogger(__name__)

_TPE_TZ = timezone(timedelta(hours=8))

# Push schedule. Every 5 minutes up to 09:30 (where the call firms up for
# clearly-abnormal days), then two follow-ups as the remaining days resolve.
_PUSH_TIMES = (
    dtime(9, 10), dtime(9, 15), dtime(9, 20), dtime(9, 25), dtime(9, 30),
    dtime(10, 0), dtime(10, 30),
)
# Jobs fire 30s past the minute so the sweeper (20s cadence) has had time to
# write the bucket we're about to read.
_PUSH_OFFSET_S = 30

# 09:05 is deliberately absent: with a single bucket the projection's median
# error is 26% and its p90 is 256%. These two are pushed but flagged.
_NOISY_UNTIL = dtime(9, 15)

# A bucket older than this means the sweeper stopped writing (service down,
# session ended early). The brief still goes out — silence would read as "no
# news" — but says so, rather than passing stale numbers off as current.
_STALE_AFTER_MIN = 10

# Full-day projection vs the 5-day average. Tiers are the measured accuracy
# bands described in the module docstring.
_DECIDED_DEV = 0.15
_FLAT_DEV = 0.05

# 13:30-bucket → official-close multiplier: median over this many prior days,
# falling back to 1.0 below the minimum (see module docstring).
_CAL_LOOKBACK = 20
_CAL_MIN_DAYS = 5

JOB_NAME = "market_volume_brief"

_YI = 100_000_000  # 億

# Today's previously pushed projection, for the direction arrow.
# {trade_date: projected_value}
_last_projection: dict[date, float] = {}


# ── Helpers ───────────────────────────────────────────────────────────────


def _now_tpe() -> datetime:
    return datetime.now(_TPE_TZ)


def _is_trading_day(d: date) -> bool:
    return d.weekday() < 5


def _fmt_yi(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value / _YI:,.0f} 億"


def _fmt_pct(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:+.2f}%"


def _fmt_dev(dev: float) -> str:
    return f"{dev * 100:+.1f}%"


# ── DB access ─────────────────────────────────────────────────────────────


def _open_bucket(now: datetime) -> str:
    """The bucket the sweeper is currently filling, mirroring its labelling.

    Buckets are labelled by the time they *end* (a sweep at 09:10:25 writes
    the '09:15' row), so the newest row in the table is always a partial one.
    Reading it would divide a few seconds of turnover by the full-bucket h(t)
    and understate the projection badly — at 09:10:30 that read 30 seconds of
    data as if it were 15 minutes' worth.
    """
    m = now.minute - (now.minute % 5) + 5
    h = now.hour
    if m >= 60:
        h += 1
        m -= 60
    return f"{h:02d}:{m:02d}"


def _load_latest_bucket(trade_date: date, before: str | None = None) -> dict | None:
    """The most recent *complete* profile row for trade_date, or None if the
    sweeper hasn't written anything yet (holiday, or service down).

    before: exclude this bucket and everything after it — pass the currently
    filling bucket so we only read settled ones.

    Buckets past 13:30 are excluded too: the sweeper keeps writing until its
    13:50 cutoff, but h(t) is only defined up to the close.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT time_bucket, market_total_value, twse_trade_value,
                   tpex_trade_value, taiex_close, taiex_change_pct
            FROM tw.intraday_value_profile
            WHERE trade_date = %s
              AND time_bucket <= '13:30'
              AND (%s IS NULL OR time_bucket < %s)
            ORDER BY time_bucket DESC
            LIMIT 1
            """,
            (trade_date, before, before),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _load_official_baselines(trade_date: date) -> tuple[float | None, float | None]:
    """(previous day, 5-day average) TWSE turnover from tw.index_prices.

    Same publisher and same definition as IX0001's tradeValue, so the
    comparison is like-for-like.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT turnover
            FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date < %s AND turnover IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 5
            """,
            (trade_date,),
        )
        values = [float(r["turnover"]) for r in cur.fetchall()]
    if not values:
        return None, None
    return values[0], sum(values) / len(values)


def _load_tpex_baselines(trade_date: date) -> tuple[float | None, float | None]:
    """(previous day, 5-day average) TPEx turnover, summed from daily_prices.

    tw.index_prices carries no turnover for TPEx, so this is the only series
    with history. It tracks IX0043's own figure closely — 1,925 vs 1,928 億 on
    2026-08-10, a 0.2% gap — so the two are comparable in practice.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT dp.trade_date, SUM(dp.turnover) AS turnover
            FROM tw.daily_prices dp
            JOIN tw.stocks s ON s.stock_id = dp.stock_id
            WHERE s.market = 'TPEx' AND dp.trade_date < %s
            GROUP BY dp.trade_date
            ORDER BY dp.trade_date DESC
            LIMIT 5
            """,
            (trade_date,),
        )
        values = [float(r["turnover"]) for r in cur.fetchall() if r["turnover"]]
    if not values:
        return None, None
    return values[0], sum(values) / len(values)


def _load_profile_baselines(trade_date: date) -> tuple[float | None, float | None]:
    """Same two baselines from our own summed series, for the degraded path
    where the authoritative index columns are missing."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT market_total_value
            FROM tw.intraday_value_profile
            WHERE trade_date < %s AND time_bucket = '13:30'
            ORDER BY trade_date DESC
            LIMIT 5
            """,
            (trade_date,),
        )
        values = [float(r["market_total_value"]) for r in cur.fetchall()]
    if not values:
        return None, None
    return values[0], sum(values) / len(values)


def _load_calibration(trade_date: date) -> tuple[float, float]:
    """(TWSE, TPEx) multipliers from the 13:30 bucket to the official close.

    Median of the last _CAL_LOOKBACK days before trade_date, so a replay of a
    past day never sees its own close. TPEx's close is the daily_prices sum,
    the same series its baselines come from.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT i.turnover::numeric / p.twse_trade_value AS k
            FROM tw.intraday_value_profile p
            JOIN tw.index_prices i
              ON i.index_id = 'TAIEX' AND i.trade_date = p.trade_date
            WHERE p.time_bucket = '13:30' AND p.trade_date < %s
              AND p.twse_trade_value > 0 AND i.turnover IS NOT NULL
            ORDER BY p.trade_date DESC
            LIMIT %s
            """,
            (trade_date, _CAL_LOOKBACK),
        )
        twse = [float(r["k"]) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT SUM(dp.turnover) / MAX(p.tpex_trade_value) AS k
            FROM tw.intraday_value_profile p
            JOIN tw.daily_prices dp ON dp.trade_date = p.trade_date
            JOIN tw.stocks s ON s.stock_id = dp.stock_id AND s.market = 'TPEx'
            WHERE p.time_bucket = '13:30' AND p.trade_date < %s
              AND p.tpex_trade_value > 0
            GROUP BY p.trade_date
            ORDER BY p.trade_date DESC
            LIMIT %s
            """,
            (trade_date, _CAL_LOOKBACK),
        )
        tpex = [float(r["k"]) for r in cur.fetchall() if r["k"]]
    k_twse = median(twse) if len(twse) >= _CAL_MIN_DAYS else 1.0
    k_tpex = median(tpex) if len(tpex) >= _CAL_MIN_DAYS else 1.0
    return k_twse, k_tpex


def _load_official_actual(trade_date: date) -> tuple[float | None, float | None]:
    """(TWSE, TPEx) official full-day turnover for trade_date — None until the
    EOD pipeline has written that day."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT turnover FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date = %s
            """,
            (trade_date,),
        )
        row = cur.fetchone()
        twse = float(row["turnover"]) if row and row["turnover"] else None
        cur.execute(
            """
            SELECT SUM(dp.turnover) AS turnover
            FROM tw.daily_prices dp
            JOIN tw.stocks s ON s.stock_id = dp.stock_id
            WHERE s.market = 'TPEx' AND dp.trade_date = %s
            """,
            (trade_date,),
        )
        row = cur.fetchone()
        tpex = float(row["turnover"]) if row and row["turnover"] else None
    return twse, tpex


# ── Message ───────────────────────────────────────────────────────────────


def _verdict(dev: float) -> str:
    direction = "量增" if dev > 0 else "量縮"
    if abs(dev) >= _DECIDED_DEV:
        return f"{direction}（已定調）"
    if abs(dev) >= _FLAT_DEV:
        return f"傾向{direction}（信心中）"
    return "與近期相當（此刻判不準）"


def _close_verdict(dev: float) -> str:
    """After the close the figure is settled, so the same bands grade size, not confidence."""
    direction = "量增" if dev > 0 else "量縮"
    if abs(dev) >= _DECIDED_DEV:
        return f"明顯{direction}"
    if abs(dev) >= _FLAT_DEV:
        return direction
    return "與近期相當"


# The three builders below are shared with /market's 量能 section so the push
# and the command read identically.


def _vs(value: float, prev_day: float | None, ma5: float | None) -> str:
    """'昨 -5.2%／5日均 -12.0%' (either half dropped when its baseline is missing)."""
    segs = []
    if prev_day:
        segs.append(f"昨 {_fmt_dev(value / prev_day - 1)}")
    if ma5:
        segs.append(f"5日均 {_fmt_dev(value / ma5 - 1)}")
    return "／".join(segs)


def _mkt_line(name: str, value: float, prev_day: float | None, ma5: float | None,
              approx: bool = False, amount: bool = True) -> str:
    """'  上市 7,048 億（昨 -6.1%／5日均 -18.9%）' — 上市 and 上櫃 share one layout.
    approx prefixes 約; amount=False when only the ratios are trustworthy."""
    amt = f"{'約 ' if approx else ''}{_fmt_yi(value)}" if amount else "金額暫缺"
    vs = _vs(value, prev_day, ma5)
    return f"  {name} {amt}（{vs}）" if vs else f"  {name} {amt}"


def _verdict_line(projected: float, ma5: float,
                  tpex: float | None, tpex_ma5: float | None, closed: bool = False) -> str:
    # The 5%/15% bands held on TPEx too: decided days called right 8/8 at
    # both 09:30 and 10:30 over 2026-08-24..09-10 (n=13, still thin).
    grade = _close_verdict if closed else _verdict
    verdict = f"上市{grade(projected / ma5 - 1)}"
    if tpex and tpex_ma5:
        verdict += f"／上櫃{grade(tpex / tpex_ma5 - 1)}"
    return f"  判定：{verdict}"


def _build_message(
    bucket: str,
    h: float,
    projected: float,
    prev_projected: float | None,
    prev_day: float,
    ma5: float,
    taiex_close: float | None,
    taiex_change_pct: float | None,
    tpex_projected: float | None,
    official: bool,
    stale_min: int = 0,
    tpex_prev_day: float | None = None,
    tpex_ma5: float | None = None,
) -> str:
    # The arrow quotes an absolute figure, so it is dropped when amounts
    # aren't authoritative this bucket.
    arrow = ""
    if prev_projected and official:
        if projected > prev_projected * 1.005:
            arrow = f"  ↑上則 {prev_projected / _YI:,.0f}"
        elif projected < prev_projected * 0.995:
            arrow = f"  ↓上則 {prev_projected / _YI:,.0f}"
        else:
            arrow = "  →持平"

    flags = ""
    if dtime.fromisoformat(bucket) <= _NOISY_UNTIL:
        flags += " ⚠ 初步、雜訊大"
    if stale_min >= _STALE_AFTER_MIN:
        flags += f" ⚠ 資料延遲 {stale_min} 分"
    head = [f"[市場量能] {bucket} 盤中預估  h={h * 100:.0f}%{arrow}{flags}"]

    if taiex_close is not None:
        # Taiwan convention: 紅漲 🔴 / 綠跌 🟢
        mark = "🔴" if (taiex_change_pct or 0) > 0 else "🟢" if (taiex_change_pct or 0) < 0 else "⚪"
        head.append(f"加權 {taiex_close:,.0f}  {mark} {_fmt_pct(taiex_change_pct)}")

    body = [_mkt_line("上市", projected, prev_day, ma5, amount=official)]
    if official and tpex_projected:
        body.append(_mkt_line("上櫃", tpex_projected, tpex_prev_day, tpex_ma5))
    body.append(_verdict_line(projected, ma5, tpex_projected if official else None, tpex_ma5))

    return "\n".join(head) + "\n\n" + "\n".join(body)


# ── Orchestration ─────────────────────────────────────────────────────────


def compute_projection(now: datetime) -> tuple[dict | None, str]:
    """Full-day turnover projection as of `now`, with the baselines to judge it.

    Read-only, so /market shows the same figures the push sends. Uses today's
    settled buckets when there are any, otherwise the most recent earlier day
    with data (pre-open, weekends, holidays). Returns (projection, status);
    projection is None when there is nothing to project.
    """
    trade_date = now.date()
    # The open bucket is skipped so a mid-bucket read never divides a few
    # seconds of turnover by a full bucket's h(t).
    row = _load_latest_bucket(trade_date, before=_open_bucket(now))
    if row is None:
        with get_cursor(commit=False) as cur:
            cur.execute(
                "SELECT MAX(trade_date) AS d FROM tw.intraday_value_profile WHERE trade_date < %s",
                (trade_date,),
            )
            trade_date = cur.fetchone()["d"]
        if trade_date is None:
            return None, "intraday_value_profile is empty"
        row = _load_latest_bucket(trade_date)
        if row is None:
            return None, f"no settled bucket for {trade_date}"

    bucket = row["time_bucket"]
    h = get_h_curve().get(bucket)
    if not h or h <= 0:
        return None, f"h(t) unavailable for bucket {bucket}"

    twse_value = row["twse_trade_value"]
    official = twse_value is not None
    k_twse, k_tpex = _load_calibration(trade_date) if official else (1.0, 1.0)
    if official:
        prev_day, ma5 = _load_official_baselines(trade_date)
        current = float(twse_value) * k_twse
    else:
        # Our own summed series against its own 13:30 baselines — already
        # same-basis, so no calibration.
        prev_day, ma5 = _load_profile_baselines(trade_date)
        current = float(row["market_total_value"])

    if not prev_day or not ma5:
        return None, "no turnover baseline available"

    tpex_prev_day, tpex_ma5 = _load_tpex_baselines(trade_date) if official else (None, None)
    tpex_value = row["tpex_trade_value"]
    actual, tpex_actual = _load_official_actual(trade_date) if official else (None, None)

    # Only meaningful for today; an earlier trade_date is expected to be old.
    stale_min = 0
    if trade_date == now.date():
        bucket_at = datetime.combine(trade_date, dtime.fromisoformat(bucket), tzinfo=_TPE_TZ)
        stale_min = max(0, int((now - bucket_at).total_seconds() // 60))

    projection = {
        "trade_date": trade_date,
        "bucket": bucket,
        "h": h,
        "projected": current / h,
        "prev_day": prev_day,
        "ma5": ma5,
        "official": official,
        "actual": actual,
        "taiex_close": float(row["taiex_close"]) if row["taiex_close"] is not None else None,
        "taiex_change_pct": (
            float(row["taiex_change_pct"]) if row["taiex_change_pct"] is not None else None
        ),
        "tpex_projected": float(tpex_value) * k_tpex / h if official and tpex_value else None,
        "tpex_actual": tpex_actual,
        "tpex_prev_day": tpex_prev_day,
        "tpex_ma5": tpex_ma5,
        "stale_min": stale_min,
    }
    return projection, f"bucket={bucket}, official={official}, k={k_twse:.3f}/{k_tpex:.3f}"


def _run_check(respect_gates: bool = True) -> tuple[str | None, str]:
    """Build and (optionally) push one brief. Returns (message, status)."""
    now = _now_tpe()
    today = now.date()

    if respect_gates and not _is_trading_day(today):
        return None, "not a trading day; skipped"

    p, status = compute_projection(now)
    if p is None:
        return None, status
    # A live push only ever speaks about today; the CLI may preview an older day.
    if respect_gates and p["trade_date"] != today:
        return None, f"no profile rows for {today}; skipped"

    message = _build_message(
        bucket=p["bucket"],
        h=p["h"],
        projected=p["projected"],
        prev_projected=_last_projection.get(p["trade_date"]),
        prev_day=p["prev_day"],
        ma5=p["ma5"],
        taiex_close=p["taiex_close"],
        taiex_change_pct=p["taiex_change_pct"],
        tpex_projected=p["tpex_projected"],
        official=p["official"],
        stale_min=p["stale_min"],
        tpex_prev_day=p["tpex_prev_day"],
        tpex_ma5=p["tpex_ma5"],
    )

    _last_projection.clear()  # single-entry table: today's value only
    _last_projection[p["trade_date"]] = p["projected"]

    ok = send_sync(message) if respect_gates else True
    return message, f"{status}, send_ok={ok}"


async def check_once(context) -> None:  # noqa: ANN001 — PTB context
    try:
        _, status = await asyncio.to_thread(_run_check, True)
        logger.info("market_volume_brief: %s", status)
    except Exception as exc:
        logger.exception("market_volume_brief crashed: %s", exc)


# ── Wiring ────────────────────────────────────────────────────────────────


def register(application) -> None:  # noqa: ANN001 — PTB Application
    jq = application.job_queue
    if jq is None:
        logger.warning(
            "JobQueue unavailable; market_volume_brief not scheduled. "
            "Install python-telegram-bot[job-queue]."
        )
        return
    for t in _PUSH_TIMES:
        at = (
            datetime.combine(date.today(), t) + timedelta(seconds=_PUSH_OFFSET_S)
        ).time().replace(tzinfo=_TPE_TZ)
        jq.run_daily(
            check_once,
            time=at,
            name=f"{JOB_NAME}_{t.strftime('%H%M')}",
        )
    logger.info(
        "Scheduled %s daily at %s TPE (+%ds, weekend self-gated)",
        JOB_NAME,
        ", ".join(t.strftime("%H:%M") for t in _PUSH_TIMES),
        _PUSH_OFFSET_S,
    )


# ── CLI ───────────────────────────────────────────────────────────────────


def _cli() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    msg, status = _run_check(respect_gates=False)
    print(f"[market_volume_brief] {status}")
    if msg:
        print()
        print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
