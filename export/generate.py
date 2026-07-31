"""
Generate static JSON files for the frontend from DB data.

Usage:
  python -m export.generate                # output to frontend/public/data/
  python -m export.generate ./out          # output to custom dir
"""

import json
import os
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from db.connection import get_cursor, init_db


def _disposal_status_for(ticker: str, freshness) -> str:
    """Best-effort disposal status string for a single ticker.

    Imports lazily so this module doesn't hard-depend on telegram_bot at
    import time (telegram_bot pulls PTB which may not be installed in some
    deployment slices). Returns "" on any failure.

    TODO: extract disposal logic to a neutral analysis/ module so export
    doesn't reach into telegram_bot.
    """
    try:
        from telegram_bot.handlers.score import _get_disposal_status
        return _get_disposal_status(ticker, freshness, allow_refresh=False) or ""
    except Exception:
        return ""


def _current_freshness():
    try:
        from telegram_bot.handlers._data_freshness import detect_state
        return detect_state()
    except Exception:
        return None


def _serial(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _write(data, path: Path):
    """Atomically write JSON to ``path``.

    Writes to a sibling tempfile in the same directory and ``os.replace``s
    onto the final path so a concurrent reader (publish.bat / Telegram
    push) never sees a half-written JSON. Same-directory replace is
    atomic on both POSIX and Windows.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=_serial, indent=2)
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    print(f"  {path.name}: {path.stat().st_size:,} bytes")


# -----------------------------------------------------------------------
# 1. signals.json — all signals grouped by type
# -----------------------------------------------------------------------

def export_signals(cur, out: Path):
    cur.execute("""
        SELECT s.signal_type, s.ticker, s.ticker_name, st.market,
               s.funds, s.trigger_date, s.trigger_period,
               s.weight_change, s.evidence
        FROM tw.signals s
        LEFT JOIN tw.stocks st ON st.stock_id = s.ticker
        ORDER BY s.trigger_period DESC, s.ticker
    """)
    rows = cur.fetchall()

    by_type = {}
    for r in rows:
        st_type = r["signal_type"]
        by_type.setdefault(st_type, []).append({
            "ticker": r["ticker"],
            "ticker_name": r["ticker_name"],
            "market": r["market"],
            "funds": r["funds"],
            "trigger_date": r["trigger_date"],
            "trigger_period": r["trigger_period"],
            "weight_change": float(r["weight_change"]) if r["weight_change"] else None,
            "evidence": r["evidence"],
        })

    # Period list for filters
    periods = sorted({r["trigger_period"] for r in rows}, reverse=True)

    _write({"by_type": by_type, "periods": periods}, out / "signals.json")


# -----------------------------------------------------------------------
# 3. funds.json — fund list + per-fund holdings
# -----------------------------------------------------------------------

def export_funds(cur, out: Path):
    cur.execute("""
        SELECT f.id, f.code, f.name, f.fund_type, f.company,
               fm.name AS manager_name
        FROM tw.funds f
        LEFT JOIN tw.fund_managers fm ON f.manager_id = fm.id
        ORDER BY f.fund_type, f.company, f.code
    """)
    fund_list = [dict(r) for r in cur.fetchall()]

    # Latest monthly holdings per fund
    cur.execute("SELECT MAX(period) FROM tw.fund_holdings_monthly")
    latest_m = list(cur.fetchone().values())[0]

    # Latest quarterly holdings per fund
    cur.execute("SELECT MAX(period) FROM tw.fund_holdings_quarterly")
    latest_q = list(cur.fetchone().values())[0]

    fund_holdings = {}
    for f in fund_list:
        fid = f["id"]

        if f["fund_type"] == "fund":
            # Monthly: all periods
            cur.execute("""
                SELECT h.period, h.ticker, h.ticker_name, st.market,
                       h.rank, h.weight
                FROM tw.fund_holdings_monthly h
                LEFT JOIN tw.stocks st ON st.stock_id = h.ticker
                WHERE h.fund_id = %s
                ORDER BY h.period DESC, h.rank
            """, (fid,))
            monthly = {}
            for r in cur.fetchall():
                monthly.setdefault(r["period"], []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
                    "market": r["market"],
                    "rank": r["rank"],
                    "weight": float(r["weight"]) if r["weight"] else None,
                })

            # Quarterly: all periods
            cur.execute("""
                SELECT h.period, h.ticker, h.ticker_name, st.market,
                       h.weight
                FROM tw.fund_holdings_quarterly h
                LEFT JOIN tw.stocks st ON st.stock_id = h.ticker
                WHERE h.fund_id = %s
                ORDER BY h.period DESC, h.weight DESC
            """, (fid,))
            quarterly = {}
            for r in cur.fetchall():
                quarterly.setdefault(r["period"], []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
                    "market": r["market"],
                    "weight": float(r["weight"]) if r["weight"] else None,
                })
        else:
            # ETF: use etf_holdings grouped by trade_date
            cur.execute("""
                SELECT h.trade_date, h.stock_id AS ticker,
                       h.stock_name AS ticker_name, st.market,
                       h.weight, h.shares
                FROM tw.etf_holdings h
                LEFT JOIN tw.stocks st ON st.stock_id = h.stock_id
                WHERE h.etf_id = %s
                ORDER BY h.trade_date DESC, h.weight DESC
            """, (f["code"],))
            monthly = {}
            for r in cur.fetchall():
                key = str(r["trade_date"])
                monthly.setdefault(key, []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
                    "market": r["market"],
                    "rank": None,
                    "weight": float(r["weight"]) if r["weight"] else None,
                    "shares": r["shares"],
                })
            quarterly = {}

        fund_holdings[f["code"]] = {
            "monthly": monthly,
            "quarterly": quarterly,
        }

    _write({
        "funds": fund_list,
        "holdings": fund_holdings,
        "latest_monthly": latest_m,
        "latest_quarterly": latest_q,
    }, out / "funds.json")


# -----------------------------------------------------------------------
# 4. dual_track.json — fund vs ETF side-by-side (same manager)
# -----------------------------------------------------------------------

def export_dual_track(cur, out: Path):
    # Same-manager fund-ETF pairs
    cur.execute("""
        SELECT f1.code AS fund_code, f1.name AS fund_name,
               f2.code AS etf_code, f2.name AS etf_name,
               fm.name AS manager
        FROM tw.funds f1
        JOIN tw.funds f2 ON f1.manager_id = f2.manager_id AND f1.id != f2.id
        JOIN tw.fund_managers fm ON f1.manager_id = fm.id
        WHERE f1.fund_type = 'fund' AND f2.fund_type = 'etf'
        ORDER BY f1.company, fm.name
    """)
    pairs = [dict(r) for r in cur.fetchall()]

    # Latest monthly period
    cur.execute("SELECT MAX(period) FROM tw.fund_holdings_monthly")
    latest_m = list(cur.fetchone().values())[0]

    # For each pair, get fund monthly top-10 and ETF latest holdings
    for pair in pairs:
        # Fund monthly
        cur.execute("""
            SELECT m.ticker, m.ticker_name, m.rank, m.weight
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE f.code = %s AND m.period = %s
            ORDER BY m.rank
        """, (pair["fund_code"], latest_m))
        pair["fund_holdings"] = [dict(r) for r in cur.fetchall()]

        # ETF latest holdings
        cur.execute("""
            SELECT stock_id AS ticker, stock_name AS ticker_name,
                   weight, shares
            FROM tw.etf_holdings
            WHERE etf_id = %s AND trade_date = (
                SELECT MAX(trade_date) FROM tw.etf_holdings WHERE etf_id = %s
            )
            ORDER BY weight DESC
        """, (pair["etf_code"], pair["etf_code"]))
        pair["etf_holdings"] = [dict(r) for r in cur.fetchall()]

        # Overlap: tickers in both
        fund_tickers = {h["ticker"] for h in pair["fund_holdings"]}
        etf_tickers = {h["ticker"] for h in pair["etf_holdings"]}
        pair["overlap"] = sorted(fund_tickers & etf_tickers)

    _write({
        "pairs": pairs,
        "latest_monthly": latest_m,
    }, out / "dual_track.json")


# -----------------------------------------------------------------------
# 8. flow.json — cross-fund weight changes heatmap
# -----------------------------------------------------------------------

def export_flow(cur, out: Path):
    cur.execute("""
        SELECT DISTINCT period FROM tw.fund_holdings_monthly ORDER BY period
    """)
    all_periods = [r["period"] for r in cur.fetchall()]
    if len(all_periods) < 2:
        _write({"periods": all_periods, "fund_columns": [], "changes": {}}, out / "flow.json")
        return

    # Last up to 4 month-ends → up to 3 monthly transitions.
    periods = all_periods[-4:]
    latest, prev = periods[-1], periods[-2]

    # Month-end closing prices per period, for share estimation from holding amount.
    def _month_prices(period):
        y, m = int(period[:4]), int(period[4:])
        start = f"{y}-{m:02d}-01"
        end = f"{y + 1}-01-01" if m == 12 else f"{y}-{m + 1:02d}-01"
        cur.execute("""
            SELECT DISTINCT ON (stock_id) stock_id, close_price
            FROM tw.daily_prices
            WHERE trade_date >= %s AND trade_date < %s
              AND close_price IS NOT NULL
            ORDER BY stock_id, trade_date DESC
        """, (start, end))
        return {r["stock_id"]: float(r["close_price"]) for r in cur.fetchall()}

    prices = {p: _month_prices(p) for p in periods}

    # All 'fund'-type holdings across the selected periods.
    cur.execute("""
        SELECT c.ticker, c.ticker_name, st.market, c.period,
               f.code AS fund_code, c.amount, c.weight
        FROM tw.fund_holdings_monthly c
        JOIN tw.funds f ON c.fund_id = f.id AND f.fund_type = 'fund'
        LEFT JOIN tw.stocks st ON st.stock_id = c.ticker
        WHERE c.period = ANY(%s)
        ORDER BY c.ticker, f.code, c.period
    """, (periods,))

    # ticker -> fund -> period -> {shares, weight}
    info: dict[str, dict] = {}
    holds: dict[str, dict[str, dict[str, dict]]] = {}
    for r in cur.fetchall():
        t = r["ticker"]
        info.setdefault(t, {"ticker_name": r["ticker_name"], "market": r["market"]})
        price = prices.get(r["period"], {}).get(t)
        shares = round(r["amount"] / price) if r["amount"] and price else None
        holds.setdefault(t, {}).setdefault(r["fund_code"], {})[r["period"]] = {
            "shares": shares,
            "weight": float(r["weight"]) if r["weight"] else None,
        }

    def _share_delta(s0, s1):
        if s1 is not None and s0 is not None:
            return s1 - s0
        if s1 is not None:
            return s1   # new position
        if s0 is not None:
            return -s0  # full exit
        return None

    changes = {}
    for t, fundmap in holds.items():
        # Net shares across all funds for each month transition.
        monthly_net = []
        for i in range(1, len(periods)):
            p0, p1 = periods[i - 1], periods[i]
            net = 0
            for pmap in fundmap.values():
                d = _share_delta(
                    (pmap.get(p0) or {}).get("shares"),
                    (pmap.get(p1) or {}).get("shares"),
                )
                if d is not None:
                    net += d
            monthly_net.append(net)

        # Latest-transition per-fund detail for the breakdown table.
        funds_detail = {}
        for fc, pmap in fundmap.items():
            cur_h, prev_h = pmap.get(latest), pmap.get(prev)
            if not cur_h and not prev_h:
                continue
            diff = _share_delta(
                (prev_h or {}).get("shares"),
                (cur_h or {}).get("shares"),
            )
            funds_detail[fc] = {
                "curr": (cur_h or {}).get("weight"),
                "prev": (prev_h or {}).get("weight"),
                "diff": diff,
            }

        changes[t] = {
            "ticker_name": info[t]["ticker_name"],
            "market": info[t]["market"],
            "monthly_net": monthly_net,      # net shares per transition
            "total_net": sum(monthly_net),   # cumulative net shares
            "funds": funds_detail,
        }

    cur.execute("SELECT code, name FROM tw.funds WHERE fund_type='fund' ORDER BY company")
    fund_cols = [dict(r) for r in cur.fetchall()]

    _write({
        "periods": periods,
        "fund_columns": fund_cols,
        "changes": changes,
    }, out / "flow.json")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def export_hermit(cur, out: Path):
    """Top picks from hermit_stock daily snapshot (latest date + 30d history)."""
    cur.execute("""
        SELECT MAX(snapshot_date) AS d FROM tw.hermit_screen_snapshot
    """)
    latest = cur.fetchone()["d"]
    if latest is None:
        _write({"snapshot_date": None, "picks": [], "history": []}, out / "hermit.json")
        return

    # Latest snapshot rows joined with stock name/industry
    cur.execute("""
        SELECT s.rank, s.stock_id, st.name, st.market, st.industry,
               s.score, s.grade,
               s.f1_pass, s.f2_pass, s.f3_pass, s.f4_pass,
               s.f5_pass, s.f6_pass, s.f7_pass, s.f8_pass,
               s.val_method, s.val_multiple, s.val_band,
               s.val_upside_pct, s.val_decision,
               s.is_new, s.prev_rank, s.rank_delta
        FROM tw.hermit_screen_snapshot s
        JOIN tw.stocks st ON st.stock_id = s.stock_id
        WHERE s.snapshot_date = %s
        ORDER BY s.rank
    """, (latest,))
    picks = []
    for r in cur.fetchall():
        picks.append({
            "rank": r["rank"],
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "industry": r["industry"],
            "score": r["score"],
            "grade": r["grade"],
            "rules": {
                f"F{i}": r[f"f{i}_pass"] for i in range(1, 9)
            },
            "valuation": {
                "method": r["val_method"],
                "multiple": float(r["val_multiple"]) if r["val_multiple"] is not None else None,
                "band": r["val_band"],
                "upside_pct": float(r["val_upside_pct"]) if r["val_upside_pct"] is not None else None,
                "decision": r["val_decision"],
            },
            "is_new": r["is_new"],
            "prev_rank": r["prev_rank"],
            "rank_delta": r["rank_delta"],
        })

    # Last 30 trading-snapshot dates with diff summaries
    cur.execute("""
        SELECT snapshot_date,
               COUNT(*) AS top_n,
               SUM(CASE WHEN is_new THEN 1 ELSE 0 END) AS new_count
        FROM tw.hermit_screen_snapshot
        WHERE snapshot_date > %s - INTERVAL '60 days'
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT 30
    """, (latest,))
    history = [
        {
            "date": r["snapshot_date"],
            "top_n": r["top_n"],
            "new_count": int(r["new_count"]) if r["new_count"] is not None else 0,
        }
        for r in cur.fetchall()
    ]

    _write({
        "snapshot_date": latest,
        "picks": picks,
        "history": history,
    }, out / "hermit.json")


# Definitions for the 8 monthly-revenue screeners. The screen() functions live
# in analysis/revenue_*.py; we reuse them as-is and just pull metadata + the
# column layout the frontend should render.
_REVENUE_STRATEGIES = [
    {
        "key": "three_arrows",
        "label": "營收三支箭",
        "side": "long",
        "description": "當月營收 > 歷史均值 + YoY 加速 + 歷史新高",
        "module": "analysis.revenue_three_arrows",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("rev_vs_avg_pct", "超越均值%", "pct"),
            ("yoy_pct", "單月YoY%", "pct"),
            ("cum_yoy_pct", "累計YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "turnaround",
        "label": "營收轉機股",
        "side": "long",
        "description": "YoY 連續 ≥3 個月負後當月轉正",
        "module": "analysis.revenue_turnaround",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("yoy_pct", "當月YoY%", "pct"),
            ("prev_yoy_pct", "前月YoY%", "pct"),
            ("decline_months", "前期衰退月數", "int"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "streak",
        "label": "連續成長股",
        "side": "long",
        "description": "YoY 連續 ≥6 個月為正",
        "module": "analysis.revenue_streak",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("yoy_pct", "當月YoY%", "pct"),
            ("streak", "連續成長月數", "int"),
            ("avg_yoy_pct", "期間平均YoY%", "pct"),
            ("min_yoy_pct", "期間最低YoY%", "pct"),
            ("max_yoy_pct", "期間最高YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "off_season",
        "label": "淡季不淡",
        "side": "long",
        "description": "有季節性 + 當月為淡季 + 營收高於歷年同月均值",
        "module": "analysis.revenue_off_season",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("beat_pct", "超越均值%", "pct"),
            ("seasonal_coeff", "淡季係數", "ratio3"),
            ("hist_avg", "歷年同月均值(千)", "int"),
            ("yoy_pct", "YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "deceleration",
        "label": "成長減速預警",
        "side": "short",
        "description": "連 ≥6 月 YoY 正、但 YoY% 連 ≥3 月下滑",
        "module": "analysis.revenue_deceleration",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("yoy_pct", "當月YoY%", "pct"),
            ("peak_yoy_pct", "波段最高YoY%", "pct"),
            ("yoy_drop_pct", "YoY下降幅度", "pct"),
            ("growth_streak", "連續成長月數", "int"),
            ("decel_months", "連續減速月數", "int"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "decline",
        "label": "連續衰退",
        "side": "short",
        "description": "YoY 連續 ≥6 個月為負且持續惡化",
        "module": "analysis.revenue_decline",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("yoy_pct", "當月YoY%", "pct"),
            ("streak", "連續衰退月數", "int"),
            ("avg_yoy_pct", "期間平均YoY%", "pct"),
            ("worst_yoy_pct", "期間最差YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "historic_low",
        "label": "營收歷史新低",
        "side": "short",
        "description": "當月營收為歷史最低（≥24 個月歷史）",
        "module": "analysis.revenue_historic_low",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("prev_min_revenue", "前歷史低(千)", "int"),
            ("below_avg_pct", "低於均值%", "pct"),
            ("yoy_pct", "YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
    {
        "key": "peak_miss",
        "label": "旺季不旺",
        "side": "short",
        "description": "有季節性 + 當月為旺季 + 營收低於歷年同月均值",
        "module": "analysis.revenue_peak_miss",
        "columns": [
            ("stock_id", "代號", None),
            ("name", "名稱", None),
            ("industry", "產業", None),
            ("revenue", "當月營收(千)", "int"),
            ("miss_pct", "落後均值%", "pct"),
            ("seasonal_coeff", "旺季係數", "ratio3"),
            ("hist_avg", "歷年同月均值(千)", "int"),
            ("yoy_pct", "YoY%", "pct"),
            ("mom_pct", "MoM%", "pct"),
        ],
    },
]


def export_revenue_screens(cur, out: Path, n_months: int = 4):
    """Run all 8 monthly-revenue screeners for the most recent n_months and
    bundle results into a single JSON for the frontend. Each screen() opens
    its own cursor via get_cursor(), so the cur argument here is only used to
    discover which months have revenue data.

    Each row carries an is_new flag: true iff the stock is in this month's
    screen but was NOT in the previous month's screen for the same strategy
    (a new entrant vs last month). Stateless — derived purely from the screen
    membership of consecutive months.
    """
    import importlib

    cur.execute("""
        SELECT DISTINCT year_month FROM tw.monthly_revenue
        ORDER BY year_month DESC
    """)
    avail_months = [r["year_month"] for r in cur.fetchall()]
    months = avail_months[:n_months]

    if not months:
        _write({
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "available_months": [],
            "strategies": [],
            "data": {},
        }, out / "revenue_screens.json")
        return

    avail_set = set(avail_months)

    def _prev_month(ym: str) -> str:
        y, m = int(ym[:4]), int(ym[5:])
        return f"{y - 1}-12" if m == 1 else f"{y}-{m - 1:02d}"

    strategies_meta = []
    for s in _REVENUE_STRATEGIES:
        strategies_meta.append({
            "key": s["key"],
            "label": s["label"],
            "side": s["side"],
            "description": s["description"],
            "columns": [
                {"key": k, "label": l, "format": fmt}
                for (k, l, fmt) in s["columns"]
            ],
        })

    # Cache screen() results so a prev-month baseline is not recomputed.
    screen_cache: dict[tuple[str, str], list] = {}

    def _screen(ym: str, s: dict) -> list:
        cache_key = (ym, s["key"])
        if cache_key not in screen_cache:
            mod = importlib.import_module(s["module"])
            try:
                screen_cache[cache_key] = mod.screen(ym)
            except Exception as e:
                print(f"  [WARN] {s['key']} {ym} failed: {e}")
                screen_cache[cache_key] = []
        return screen_cache[cache_key]

    data = {}
    for ym in months:
        data[ym] = {}
        prev_ym = _prev_month(ym)
        # If the previous month has no revenue data we cannot judge new-ness.
        prev_known = prev_ym in avail_set
        for s in _REVENUE_STRATEGIES:
            keys = [k for (k, _, _) in s["columns"]]
            prev_ids = (
                {r.get("stock_id") for r in _screen(prev_ym, s)}
                if prev_known else None
            )

            rows = []
            for r in _screen(ym, s):
                stock_id = r.get("stock_id")
                if stock_id is None:
                    continue
                row = {k: r.get(k) for k in keys}
                # Always carry market for the TradingView link, even if not
                # displayed as a column.
                row["market"] = r.get("market")
                row["is_new"] = bool(prev_ids is not None and stock_id not in prev_ids)
                rows.append(row)

            data[ym][s["key"]] = rows

    _write({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "export_date": date.today().isoformat(),
        "available_months": months,
        "strategies": strategies_meta,
        "data": data,
    }, out / "revenue_screens.json")


def export_scores(cur, out: Path):
    """Daily ScoreBoard snapshot — top-300 long + top-300 short."""
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.score_snapshot")
    latest = cur.fetchone()["d"]
    if latest is None:
        _write({"snapshot_date": None, "long": [], "short": [], "history": []},
               out / "scores.json")
        return

    sides = {"long": [], "short": []}
    for side in ("long", "short"):
        cur.execute("""
            SELECT s.rank, s.stock_id, st.name, st.market,
                   s.total_pct, s.turnover,
                   s.is_new, s.prev_rank, s.rank_delta,
                   s.pct_d1, s.pct_d2, s.pct_d3
            FROM tw.score_snapshot s
            JOIN tw.stocks st ON st.stock_id = s.stock_id
            WHERE s.snapshot_date = %s AND s.side = %s
            ORDER BY s.rank
        """, (latest, side))
        for r in cur.fetchall():
            sides[side].append({
                "rank": r["rank"],
                "ticker": r["stock_id"],
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
            })

    cur.execute("""
        SELECT snapshot_date,
               SUM(CASE WHEN side='long'  AND is_new THEN 1 ELSE 0 END) AS new_long,
               SUM(CASE WHEN side='short' AND is_new THEN 1 ELSE 0 END) AS new_short
        FROM tw.score_snapshot
        WHERE snapshot_date > %s - INTERVAL '60 days'
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT 30
    """, (latest,))
    history = [
        {
            "date": r["snapshot_date"],
            "new_long": int(r["new_long"]) if r["new_long"] is not None else 0,
            "new_short": int(r["new_short"]) if r["new_short"] is not None else 0,
        }
        for r in cur.fetchall()
    ]

    _write({
        "snapshot_date": latest,
        "long": sides["long"],
        "short": sides["short"],
        "history": history,
    }, out / "scores.json")


_STREAK_SIGNAL_FNS: dict | None = None
_STREAK_HISTORY_DAYS = 400  # ~280 trading days, covers any realistic streak


def _get_streak_signal_fns() -> dict:
    """Lazy-load the 6 condition functions. Deferred import keeps the
    signal_backtest dependency out of generate.py's top-level for the
    common case where no export touches operations."""
    global _STREAK_SIGNAL_FNS
    if _STREAK_SIGNAL_FNS is None:
        from signal_backtest.factories._conditions import (
            pick_condition, touch_condition,
            buy_condition, sell_condition,
            buy_flee_signal, sell_flee_signal,
        )
        _STREAK_SIGNAL_FNS = {
            "pick": pick_condition,
            "touch": touch_condition,
            "buy": buy_condition,
            "sell": sell_condition,
            "buy_flee": buy_flee_signal,
            "sell_flee": sell_flee_signal,
        }
    return _STREAK_SIGNAL_FNS


def _compute_signal_streaks(
    snapshot_date,
    stock_signals: dict,
    today_via_intraday: bool = False,
) -> dict:
    """For each (signal, stock_id), recompute the strict-consecutive streak
    from snapshot_date backwards by re-running the signal factories on the
    stock's historical bars. Independent of tw.signal_snapshot data gaps.

    Args:
      snapshot_date: anchor date.
      stock_signals: {stock_id: [signal_name, ...]} firing on snapshot_date.
      today_via_intraday: True when snapshot_date's bar is not yet in
        daily_prices (intraday view). In that case streak gets +1 for today
        if (and only if) the historical streak extends through the bar
        immediately before snapshot_date.

    Returns: {(signal_name, stock_id): streak_int}
    """
    from datetime import timedelta
    from backtest.data import load_stock_data

    fns = _get_streak_signal_fns()
    start = snapshot_date - timedelta(days=_STREAK_HISTORY_DAYS)
    out: dict = {}

    for sid, signals_today in stock_signals.items():
        try:
            data = load_stock_data(sid, start_date=start, end_date=snapshot_date)
        except Exception:
            for sig in signals_today:
                out[(sig, sid)] = 1
            continue
        if data.n < 60:
            for sig in signals_today:
                out[(sig, sid)] = 1
            continue

        last_bar_is_today = (data.dates[-1] == snapshot_date)

        for sig in signals_today:
            fn = fns.get(sig)
            if fn is None:
                out[(sig, sid)] = 1
                continue
            try:
                arr = fn(data)
            except Exception:
                out[(sig, sid)] = 1
                continue
            # Today's contribution: +1 if intraday says it fires now
            # but daily_prices doesn't yet have snapshot_date's bar.
            streak = 1 if (today_via_intraday and not last_bar_is_today) else 0
            # Count consecutive Trues backward from the last bar in data.
            for i in range(data.n - 1, -1, -1):
                if bool(arr[i]):
                    streak += 1
                else:
                    break
            out[(sig, sid)] = max(streak, 1)

    return out


def export_operations(cur, out: Path):
    """Daily signal-factory snapshot — 6 signals × stocks fired."""
    # Display order, matches analysis/signal_snapshot.py SIGNALS list.
    SIGNAL_ORDER = ["pick", "touch", "buy", "sell", "buy_flee", "sell_flee"]

    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.signal_snapshot")
    latest = cur.fetchone()["d"]
    if latest is None:
        empty = {sig: [] for sig in SIGNAL_ORDER}
        _write({"snapshot_date": None, "signals": empty}, out / "operations.json")
        return

    cur.execute("""
        SELECT s.signal, s.stock_id, st.name, st.market, s.turnover
        FROM tw.signal_snapshot s
        JOIN tw.stocks st ON st.stock_id = s.stock_id
        WHERE s.snapshot_date = %s
        ORDER BY s.turnover DESC NULLS LAST, s.stock_id
    """, (latest,))
    rows = cur.fetchall()

    # Recompute streak per (signal, stock_id) on-demand from raw price data,
    # independent of tw.signal_snapshot history (which has gaps).
    stock_signals: dict[str, list[str]] = {}
    for r in rows:
        stock_signals.setdefault(r["stock_id"], []).append(r["signal"])
    streak_map = _compute_signal_streaks(latest, stock_signals, today_via_intraday=False)

    grouped: dict[str, list] = {sig: [] for sig in SIGNAL_ORDER}
    for r in rows:
        sig = r["signal"]
        if sig not in grouped:
            continue  # safety; CHECK constraint should prevent this
        grouped[sig].append({
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
            "streak": streak_map.get((sig, r["stock_id"]), 1),
        })

    _write({
        "snapshot_date": latest,
        "signals": grouped,
    }, out / "operations.json")


def export_scores_intraday(cur, out: Path):
    """Intraday (12:50) ScoreBoard preview — top-500 long + top-500 short.

    Mirrors export_scores but reads from tw.score_snapshot_intraday and
    picks the most recent (snapshot_date, snapshot_time) tuple. DB
    persists the full alive universe per side; the LIMIT below keeps the
    JSON small for the frontend. /score replies with "名單外" for
    tickers beyond the LIMIT."""
    cur.execute("""
        SELECT snapshot_date, snapshot_time
        FROM tw.score_snapshot_intraday
        ORDER BY snapshot_date DESC, snapshot_time DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        _write({"snapshot_date": None, "snapshot_time": None,
                "long": [], "short": [], "history": []},
               out / "scores_intraday.json")
        return
    snap_date, snap_time = row["snapshot_date"], row["snapshot_time"]

    sides = {"long": [], "short": []}
    for side in ("long", "short"):
        cur.execute("""
            SELECT s.rank, s.stock_id, st.name, st.market,
                   s.total_pct, s.turnover,
                   s.is_new, s.prev_rank, s.rank_delta,
                   s.pct_d1, s.pct_d2, s.pct_d3
            FROM tw.score_snapshot_intraday s
            JOIN tw.stocks st ON st.stock_id = s.stock_id
            WHERE s.snapshot_date = %s AND s.snapshot_time = %s AND s.side = %s
            ORDER BY s.rank
            LIMIT 500
        """, (snap_date, snap_time, side))
        for r in cur.fetchall():
            sides[side].append({
                "rank": r["rank"],
                "ticker": r["stock_id"],
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
            })

    _write({
        "snapshot_date": snap_date,
        "snapshot_time": snap_time.isoformat() if snap_time is not None else None,
        "long": sides["long"],
        "short": sides["short"],
        "history": [],
    }, out / "scores_intraday.json")


def export_operations_intraday(cur, out: Path):
    """Intraday (12:50) signal-factory preview — 6 signals × stocks fired.

    Mirrors export_operations but reads from tw.signal_snapshot_intraday."""
    SIGNAL_ORDER = ["pick", "touch", "buy", "sell", "buy_flee", "sell_flee"]

    cur.execute("""
        SELECT snapshot_date, snapshot_time
        FROM tw.signal_snapshot_intraday
        ORDER BY snapshot_date DESC, snapshot_time DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        empty = {sig: [] for sig in SIGNAL_ORDER}
        _write({"snapshot_date": None, "snapshot_time": None, "signals": empty},
               out / "operations_intraday.json")
        return
    snap_date, snap_time = row["snapshot_date"], row["snapshot_time"]

    cur.execute("""
        SELECT s.signal, s.stock_id, st.name, st.market, s.turnover
        FROM tw.signal_snapshot_intraday s
        JOIN tw.stocks st ON st.stock_id = s.stock_id
        WHERE s.snapshot_date = %s AND s.snapshot_time = %s
        ORDER BY s.turnover DESC NULLS LAST, s.stock_id
    """, (snap_date, snap_time))
    rows = cur.fetchall()

    # Recompute streak on-demand. today_via_intraday=True so that when
    # daily_prices doesn't yet contain snap_date's bar (pre-EOD case), today
    # is counted as +1 against the historical streak through yesterday.
    stock_signals: dict[str, list[str]] = {}
    for r in rows:
        stock_signals.setdefault(r["stock_id"], []).append(r["signal"])
    streak_map = _compute_signal_streaks(snap_date, stock_signals, today_via_intraday=True)

    grouped: dict[str, list] = {sig: [] for sig in SIGNAL_ORDER}
    for r in rows:
        sig = r["signal"]
        if sig not in grouped:
            continue
        grouped[sig].append({
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
            "streak": streak_map.get((sig, r["stock_id"]), 1),
        })

    _write({
        "snapshot_date": snap_date,
        "snapshot_time": snap_time.isoformat() if snap_time is not None else None,
        "signals": grouped,
    }, out / "operations_intraday.json")


def export_positions_intraday(cur, out: Path):
    """Intraday (12:50) preview of unified-strategy open positions, plus
    positions that exited intraday at today's projected bar (is_exited=TRUE
    rows in tw.open_positions_intraday).

    Output structure:
      long / short          — currently open
      exited_long / exited_short — closed today, surfaced as a quick-scan list"""
    cur.execute("""
        SELECT snapshot_date, snapshot_time
        FROM tw.open_positions_intraday
        ORDER BY snapshot_date DESC, snapshot_time DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        _write({"snapshot_date": None, "snapshot_time": None,
                "long": [], "short": [],
                "exited_long": [], "exited_short": []},
               out / "positions_intraday.json")
        return
    snap_date, snap_time = row["snapshot_date"], row["snapshot_time"]

    out_data = {
        "snapshot_date": snap_date,
        "snapshot_time": snap_time.isoformat() if snap_time is not None else None,
        "long": [], "short": [],
        "exited_long": [], "exited_short": [],
    }
    freshness = _current_freshness()
    for side in ("long", "short"):
        for is_exited, key in ((False, side), (True, f"exited_{side}")):
            cur.execute("""
                SELECT p.stock_id, st.name, st.market,
                       p.entry_date, p.entry_price, p.entry_tier,
                       p.current_close, p.pnl_pct, p.bars_held, p.turnover,
                       p.defense_price, p.defense_reason, p.defense_date,
                       p.exit_reason
                FROM tw.open_positions_intraday p
                JOIN tw.stocks st ON st.stock_id = p.stock_id
                WHERE p.snapshot_date = %s AND p.snapshot_time = %s
                  AND p.side = %s AND p.is_exited = %s
                ORDER BY p.turnover DESC NULLS LAST, p.stock_id
            """, (snap_date, snap_time, side, is_exited))
            for r in cur.fetchall():
                out_data[key].append({
                    "ticker": r["stock_id"],
                    "name": r["name"],
                    "market": r["market"],
                    "entry_date": r["entry_date"],
                    "entry_price": float(r["entry_price"]),
                    "entry_tier": r["entry_tier"],
                    "current_close": float(r["current_close"]),
                    "pnl_pct": float(r["pnl_pct"]),
                    "bars_held": r["bars_held"],
                    "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
                    "defense_price": float(r["defense_price"]) if r["defense_price"] is not None else None,
                    "defense_reason": r["defense_reason"],
                    "defense_date": r["defense_date"],
                    "exit_reason": r["exit_reason"],
                    "disposal_status": _disposal_status_for(r["stock_id"], freshness),
                })

    _write(out_data, out / "positions_intraday.json")


def export_intraday(out_dir: str | None = None):
    """Standalone export entry called by intraday_snapshot.py — only
    refreshes the intraday JSONs without touching daily exports."""
    if out_dir is None:
        import sys
        if getattr(sys, 'frozen', False):
            base = Path(sys.executable).parent.parent
        else:
            base = Path(__file__).parent.parent
        out_dir = str(base / "frontend" / "public" / "data")

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Exporting intraday JSON to {out}/")
    with get_cursor(commit=False) as cur:
        export_scores_intraday(cur, out)
        export_operations_intraday(cur, out)
        export_positions_intraday(cur, out)
        # Refresh /breadth so the live 90th bar reflects this snapshot's
        # sidecar. Daily breadth/margin are owned by export_all().
        export_breadth(cur, out)
        # VIX rides on the intraday loop too — the snapshot daemon polls
        # TAIFEX MIS at most once per minute (throttled in
        # intraday_snapshot._run_pass) and we re-emit vix.json here so
        # intraday_publish.bat can pick it up alongside the other JSONs.
        export_vix(cur, out)
    # Live 溫度計 攻防 (only the stance updates intraday; the rest of
    # thermometer.json stays close-only in export_all).
    export_thermometer_stance(out)
    print("Intraday export done.")


def export_thermometer_stance(out: Path):
    """Passthrough of the intraday live-stance sidecar → thermometer_stance.json.
    Written by intraday_snapshot each pass. The frontend uses it as the live 攻防
    when its date is today, else falls back to thermometer.json's close stance."""
    sidecar = Path(__file__).parent.parent / "data" / "thermometer_stance_intraday.json"
    if not sidecar.exists():
        return
    try:
        with open(sidecar, encoding="utf-8") as f:
            stance = json.load(f)
    except Exception:
        return
    _write(stance, out / "thermometer_stance.json")


def export_positions(cur, out: Path):
    """Daily snapshot of unified-strategy open positions (long + short),
    plus positions that exited on the snapshot day (is_exited=TRUE rows
    in tw.open_positions).

    Output structure mirrors export_positions_intraday:
      long / short                 — currently open at close
      exited_long / exited_short   — closed on snapshot_date"""
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.open_positions")
    latest = cur.fetchone()["d"]
    if latest is None:
        _write({"snapshot_date": None,
                "long": [], "short": [],
                "exited_long": [], "exited_short": []},
               out / "positions.json")
        return

    out_data: dict = {
        "snapshot_date": latest,
        "long": [], "short": [],
        "exited_long": [], "exited_short": [],
    }
    freshness = _current_freshness()
    for side in ("long", "short"):
        for is_exited, key in ((False, side), (True, f"exited_{side}")):
            cur.execute("""
                SELECT p.stock_id, st.name, st.market,
                       p.entry_date, p.entry_price, p.entry_tier,
                       p.current_close, p.pnl_pct, p.bars_held, p.turnover,
                       p.defense_price, p.defense_reason, p.defense_date,
                       p.exit_reason
                FROM tw.open_positions p
                JOIN tw.stocks st ON st.stock_id = p.stock_id
                WHERE p.snapshot_date = %s AND p.side = %s AND p.is_exited = %s
                ORDER BY p.turnover DESC NULLS LAST, p.stock_id
            """, (latest, side, is_exited))
            for r in cur.fetchall():
                out_data[key].append({
                    "ticker": r["stock_id"],
                    "name": r["name"],
                    "market": r["market"],
                    "entry_date": r["entry_date"],
                    "entry_price": float(r["entry_price"]),
                    "entry_tier": r["entry_tier"],
                    "current_close": float(r["current_close"]),
                    "pnl_pct": float(r["pnl_pct"]),
                    "bars_held": r["bars_held"],
                    "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
                    "defense_price": float(r["defense_price"]) if r["defense_price"] is not None else None,
                    "defense_reason": r["defense_reason"],
                    "defense_date": r["defense_date"],
                    "exit_reason": r["exit_reason"],
                    "disposal_status": _disposal_status_for(r["stock_id"], freshness),
                })

    _write(out_data, out / "positions.json")


BREADTH_WINDOW_DAYS = 89
MARGIN_WINDOW_DAYS = 233
MARGIN_STAT_WINDOW_DAYS = 752   # ~3 years; basis for percentile + rolling stats
MARGIN_MA_WINDOW = 55           # ~quarterly Fibonacci-aligned MA + std for the band


def _breadth_row_from_counts(
    trade_date,
    total,
    short_up, short_down,
    medium_up, medium_down,
    long_up, long_down,
    short_trend=None, medium_trend=None, long_trend=None,
    is_intraday=False,
    intraday_time=None,
):
    s_up = short_up / total if total else 0
    s_dn = short_down / total if total else 0
    m_up = medium_up / total if total else 0
    m_dn = medium_down / total if total else 0
    l_up = long_up / total if total else 0
    l_dn = long_down / total if total else 0
    row = {
        "date": trade_date,
        "total": total,
        "s_up":  round(s_up, 6),  "s_dn": round(s_dn, 6),
        "s_neu": round(max(0.0, 1 - s_up - s_dn), 6),
        "m_up":  round(m_up, 6),  "m_dn": round(m_dn, 6),
        "m_neu": round(max(0.0, 1 - m_up - m_dn), 6),
        "l_up":  round(l_up, 6),  "l_dn": round(l_dn, 6),
        "l_neu": round(max(0.0, 1 - l_up - l_dn), 6),
        "s_trend": short_trend,
        "m_trend": medium_trend,
        "l_trend": long_trend,
    }
    if is_intraday:
        row["is_intraday"] = True
        if intraday_time is not None:
            row["intraday_time"] = intraday_time
    return row


def export_breadth(cur, out: Path):
    """Daily market-wide 多空頭排列 ratios (short/medium/long), last 89 trading days.

    For each trading date, emits (up_pct, down_pct, neutral_pct) where:
      up_pct       = short_up / active_stocks
      down_pct     = short_down / active_stocks
      neutral_pct  = 1 - up_pct - down_pct
    and analogously for medium / long. Mirrors the Excel reference
    `MarketCompany.xlsm` which charts H/I/O (short), J/K/P (medium),
    L/M/Q (long) over time.

    Intraday append: if a sidecar `data/breadth_intraday.json` exists and
    its date is newer than the latest daily date, it is appended as the
    90th bar with `is_intraday=true` so the live trading-hours view
    extends seamlessly past the last close.
    """
    cur.execute(
        """
        SELECT trade_date, total_stocks,
               short_up_total, short_down_total,
               medium_up_total, medium_down_total,
               long_up_total, long_down_total,
               short_trend_total, medium_trend_total, long_trend_total
        FROM tw.market_breadth
        WHERE total_stocks > 0
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (BREADTH_WINDOW_DAYS,),
    )
    rows = list(reversed(cur.fetchall()))
    series = [
        _breadth_row_from_counts(
            r["trade_date"], r["total_stocks"],
            r["short_up_total"], r["short_down_total"],
            r["medium_up_total"], r["medium_down_total"],
            r["long_up_total"], r["long_down_total"],
            r["short_trend_total"], r["medium_trend_total"], r["long_trend_total"],
        )
        for r in rows
    ]

    latest_daily = series[-1]["date"] if series else None

    # Append intraday bar from sidecar if newer than latest close.
    sidecar = Path(__file__).parent.parent / "data" / "breadth_intraday.json"
    if sidecar.exists():
        try:
            with open(sidecar, encoding="utf-8") as f:
                ib = json.load(f)
            ib_date = ib.get("trade_date")
            ib_total = ib.get("total", ib.get("active"))  # legacy fallback
            if ib_date and (latest_daily is None or str(ib_date) > str(latest_daily)):
                # Re-derive trend from ratios using existing classifier so the
                # intraday bar carries the same trend cells the cards show.
                from analysis.market_breadth import classify_trend, TREND_CODE
                s_up_pct = ib["short_up"] / ib_total * 100 if ib_total else 0
                s_dn_pct = ib["short_down"] / ib_total * 100 if ib_total else 0
                m_up_pct = ib["medium_up"] / ib_total * 100 if ib_total else 0
                m_dn_pct = ib["medium_down"] / ib_total * 100 if ib_total else 0
                l_up_pct = ib["long_up"] / ib_total * 100 if ib_total else 0
                l_dn_pct = ib["long_down"] / ib_total * 100 if ib_total else 0
                s_trend = TREND_CODE[classify_trend(s_up_pct, s_dn_pct, 100 - s_up_pct - s_dn_pct)]
                m_trend = TREND_CODE[classify_trend(m_up_pct, m_dn_pct, 100 - m_up_pct - m_dn_pct)]
                l_trend = TREND_CODE[classify_trend(l_up_pct, l_dn_pct, 100 - l_up_pct - l_dn_pct)]
                series.append(_breadth_row_from_counts(
                    ib_date, ib_total,
                    ib["short_up"], ib["short_down"],
                    ib["medium_up"], ib["medium_down"],
                    ib["long_up"], ib["long_down"],
                    s_trend, m_trend, l_trend,
                    is_intraday=True,
                    intraday_time=ib.get("snapshot_time"),
                ))
        except Exception as e:
            print(f"  [WARN] breadth_intraday sidecar skipped: {e}")

    _write({
        "latest_date": series[-1]["date"] if series else None,
        "series": series,
    }, out / "breadth.json")


def export_margin(cur, out: Path):
    """Daily market-wide 融資融券 statistics (tw.margin_summary).

    Surfaces both the張-balance time series and value-weighted ratios so
    the frontend can show 融資/融券餘額消長 and 資券比 over time. Units:
      margin_balance / short_balance: 張 (lots)
      margin_balance_value: 仟元 (thousands NTD)
      short_to_margin_pct = short_balance / margin_balance * 100%
    """
    cur.execute(
        """
        SELECT trade_date,
               margin_balance, margin_buy, margin_sell, margin_repay,
               margin_balance_value,
               short_balance,  short_buy, short_sell, short_repay
        FROM tw.margin_summary
        WHERE margin_balance IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (MARGIN_STAT_WINDOW_DAYS,),
    )
    rows = list(reversed(cur.fetchall()))

    import numpy as np
    n = len(rows)
    # 用「金額」(margin_balance_value, 仟元) 算水位 — 反映真實槓桿規模，
    # 不會被股價膨脹推高。「張」維持在時序圖供活絡度參考。
    mb_arr = np.array(
        [r["margin_balance_value"] if r["margin_balance_value"] is not None else np.nan for r in rows],
        dtype=float,
    )

    # Rolling 60-day MA + std for margin_balance_value, used for the ±2σ context band.
    ma60 = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    w = MARGIN_MA_WINDOW
    for i in range(w - 1, n):
        window = mb_arr[i - w + 1 : i + 1]
        valid = window[~np.isnan(window)]
        if len(valid) < w // 2:
            continue
        mu = float(valid.mean())
        sd = float(valid.std())
        ma60[i] = mu
        upper[i] = mu + 2 * sd
        lower[i] = mu - 2 * sd

    full_series = []
    for i, r in enumerate(rows):
        mb = r["margin_balance"]
        sb = r["short_balance"]
        ratio = (sb / mb * 100) if (mb and sb is not None) else None
        net_margin = None
        if r["margin_buy"] is not None and r["margin_sell"] is not None and r["margin_repay"] is not None:
            net_margin = r["margin_buy"] - r["margin_sell"] - r["margin_repay"]
        net_short = None
        if r["short_buy"] is not None and r["short_sell"] is not None and r["short_repay"] is not None:
            net_short = r["short_sell"] - r["short_buy"] - r["short_repay"]
        full_series.append({
            "date": r["trade_date"],
            "margin_balance": mb,
            "short_balance":  sb,
            "margin_balance_value": int(r["margin_balance_value"]) if r["margin_balance_value"] is not None else None,
            "short_to_margin_pct": round(ratio, 4) if ratio is not None else None,
            "net_margin": net_margin,
            "net_short": net_short,
            "ma60":  int(round(ma60[i]))  if not np.isnan(ma60[i])  else None,
            "upper": int(round(upper[i])) if not np.isnan(upper[i]) else None,
            "lower": int(round(lower[i])) if not np.isnan(lower[i]) else None,
        })

    # Slice the last 233 days for the displayed series (the rest is just
    # warm-up so ma60/upper/lower already have valid values at series[0]).
    series = full_series[-MARGIN_WINDOW_DAYS:] if len(full_series) > MARGIN_WINDOW_DAYS else full_series

    # Multi-horizon percentile rank of today's margin_balance.
    # Uses inclusive rank: P = (#<=today) / N — 50 means median, 100 means
    # tied with the highest value in the window.
    def _percentile_of(arr: np.ndarray, target: float) -> float | None:
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0 or target is None or np.isnan(target):
            return None
        return float((valid <= target).sum() / len(valid) * 100)

    stats = None
    if n and not np.isnan(mb_arr[-1]):
        latest = float(mb_arr[-1])
        # Inclusive windows ending at today (idx n-1).
        p_1m = _percentile_of(mb_arr[max(0, n - 22):n], latest)
        p_6m = _percentile_of(mb_arr[max(0, n - 132):n], latest)
        p_3y = _percentile_of(mb_arr, latest)
        # 3-year z-score (against full window) as a one-glance heat number.
        full_valid = mb_arr[~np.isnan(mb_arr)]
        z_3y = (
            float((latest - full_valid.mean()) / full_valid.std())
            if len(full_valid) >= 2 and full_valid.std() > 0
            else None
        )
        stats = {
            "stat_window_days": int(len(full_valid)),
            "ma_window": MARGIN_MA_WINDOW,
            "margin_balance_pct_1m": round(p_1m, 1) if p_1m is not None else None,
            "margin_balance_pct_6m": round(p_6m, 1) if p_6m is not None else None,
            "margin_balance_pct_3y": round(p_3y, 1) if p_3y is not None else None,
            "margin_balance_z_3y":   round(z_3y, 2) if z_3y is not None else None,
        }

    _write({
        "latest_date": series[-1]["date"] if series else None,
        "stats": stats,
        "series": series,
    }, out / "margin.json")


def export_insider_pledge(cur, out: Path):
    """Insider pledge / release events (tw.insider_pledge_events, 內部人設質解質).

    Event-grained: each row is one 設質/解質 filing. type is derived from the
    pledged/released shares (pledge / release / mixed). Last 180 days by
    change_date, joined to tw.stocks for the company name.
    """
    cur.execute(
        """
        SELECT e.stock_id, s.name AS company_name,
               e.insider_role, e.insider_name, e.change_date,
               e.pledged_shares, e.released_shares, e.cumulative_pledged,
               e.pledgee_name, e.remark, e.report_date
        FROM tw.insider_pledge_events e
        LEFT JOIN tw.stocks s ON s.stock_id = e.stock_id
        WHERE e.change_date >= CURRENT_DATE - INTERVAL '180 days'
        ORDER BY e.change_date DESC, e.stock_id
        """
    )
    rows = cur.fetchall()

    events = []
    for r in rows:
        pledged = r["pledged_shares"] or 0
        released = r["released_shares"] or 0
        if released > 0 and pledged == 0:
            ev_type = "release"
        elif pledged > 0 and released == 0:
            ev_type = "pledge"
        elif pledged > 0 and released > 0:
            ev_type = "mixed"
        else:
            ev_type = "pledge"  # both zero (rare); default label
        events.append({
            "stock_id": r["stock_id"],
            "company_name": r["company_name"],
            "insider_role": r["insider_role"],
            "insider_name": r["insider_name"],
            "change_date": r["change_date"],
            "pledged_shares": pledged,
            "released_shares": released,
            "cumulative_pledged": r["cumulative_pledged"],
            "pledgee_name": r["pledgee_name"],
            "remark": r["remark"],
            "report_date": r["report_date"],
            "type": ev_type,
        })

    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE released_shares > 0)                       AS release_events_30d,
            COUNT(DISTINCT stock_id) FILTER (WHERE released_shares > 0)       AS release_stocks_30d,
            COUNT(*) FILTER (WHERE pledged_shares > 0)                        AS pledge_events_30d
        FROM tw.insider_pledge_events
        WHERE change_date >= CURRENT_DATE - INTERVAL '30 days'
        """
    )
    s = cur.fetchone()
    stats = {
        "release_events_30d": int(s["release_events_30d"] or 0),
        "release_stocks_30d": int(s["release_stocks_30d"] or 0),
        "pledge_events_30d":  int(s["pledge_events_30d"] or 0),
    }

    _write({
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "events": events,
        "stats": stats,
    }, out / "insider_pledge.json")


def export_insider_selling(cur, out: Path):
    """Insider selling / dilution avoid-overlay (內部人賣壓/稀釋).

    Two event types, both surfaced as bearish avoid/defense flags:
      transfer  — 事前申報轉讓 with 轉讓方式=洽特定人 (insider block-sale to a specific
                  buyer); the tradeable signal (~-2.5pp/20d in liquid names).
      placement — company common-stock private placement (私募普通股); a weaker
                  small-cap dilution flag (董事會決議日 = event date).
    Last 90 days by event date, joined to tw.stocks for the company name.
    """
    _SPECIFIC = "%洽特定人%"
    _COMMON, _NOT_CB, _NOT_PREF = "%普通股%", "%轉換%", "%特別%"
    events = []

    cur.execute(
        """
        SELECT t.stock_id, s.name AS company_name, t.report_date,
               t.insider_role, t.insider_name, t.transfer_method,
               GREATEST(t.planned_shares, t.transfer_shares) AS shares,
               t.transfer_period
        FROM tw.insider_share_transfers t
        LEFT JOIN tw.stocks s ON s.stock_id = t.stock_id
        WHERE t.transfer_method LIKE %s
          AND t.report_date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY t.report_date DESC, t.stock_id
        """,
        (_SPECIFIC,),
    )
    for r in cur.fetchall():
        events.append({
            "type": "transfer",
            "stock_id": r["stock_id"],
            "company_name": r["company_name"],
            "date": r["report_date"],
            "insider_role": r["insider_role"],
            "insider_name": r["insider_name"],
            "method": r["transfer_method"],
            "shares": r["shares"] or 0,
            "security_kind": None,
            "period": r["transfer_period"],
        })

    cur.execute(
        """
        SELECT p.stock_id, s.name AS company_name, p.decide_date, p.security_kind
        FROM tw.private_placements p
        LEFT JOIN tw.stocks s ON s.stock_id = p.stock_id
        WHERE p.security_kind LIKE %s
          AND p.security_kind NOT LIKE %s
          AND p.security_kind NOT LIKE %s
          AND p.decide_date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY p.decide_date DESC, p.stock_id
        """,
        (_COMMON, _NOT_CB, _NOT_PREF),
    )
    for r in cur.fetchall():
        events.append({
            "type": "placement",
            "stock_id": r["stock_id"],
            "company_name": r["company_name"],
            "date": r["decide_date"],
            "insider_role": None,
            "insider_name": None,
            "method": None,
            "shares": None,
            "security_kind": r["security_kind"],
            "period": None,
        })

    events.sort(key=lambda e: e["date"], reverse=True)

    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE transfer_method LIKE %s)                 AS transfer_events_30d,
            COUNT(DISTINCT stock_id) FILTER (WHERE transfer_method LIKE %s)  AS transfer_stocks_30d
        FROM tw.insider_share_transfers
        WHERE report_date >= CURRENT_DATE - INTERVAL '30 days'
        """,
        (_SPECIFIC, _SPECIFIC),
    )
    t = cur.fetchone()
    cur.execute(
        """
        SELECT COUNT(*) AS placement_events_30d
        FROM tw.private_placements
        WHERE decide_date >= CURRENT_DATE - INTERVAL '30 days'
          AND security_kind LIKE %s
          AND security_kind NOT LIKE %s AND security_kind NOT LIKE %s
        """,
        (_COMMON, _NOT_CB, _NOT_PREF),
    )
    p = cur.fetchone()
    stats = {
        "transfer_events_30d":  int(t["transfer_events_30d"] or 0),
        "transfer_stocks_30d":  int(t["transfer_stocks_30d"] or 0),
        "placement_events_30d": int(p["placement_events_30d"] or 0),
    }

    _write({
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "events": events,
        "stats": stats,
    }, out / "insider_selling.json")


_FG_SUB_SLOTS = [
    "momentum", "strength", "breadth",
    "put_call", "safe_haven", "junk_bond", "volatility",
]


def export_fear_greed(cur, out: Path):
    """CNN Fear & Greed Index — last 365 days headline series + latest
    breakdown (headline rating + 7 sub-indicator raw scores & ratings)."""
    cur.execute(
        """
        SELECT trade_date, score, rating,
               momentum_score,   momentum_rating,
               strength_score,   strength_rating,
               breadth_score,    breadth_rating,
               put_call_score,   put_call_rating,
               safe_haven_score, safe_haven_rating,
               junk_bond_score,  junk_bond_rating,
               volatility_score, volatility_rating
        FROM tw.cnn_fear_greed
        ORDER BY trade_date DESC
        LIMIT 365
        """
    )
    rows = list(reversed(cur.fetchall()))
    if not rows:
        _write({"latest_date": None, "latest": None, "series": []},
               out / "fear_greed.json")
        return

    series = [
        {
            "date":  r["trade_date"],
            "score": float(r["score"]) if r["score"] is not None else None,
        }
        for r in rows
    ]

    last = rows[-1]
    sub = {}
    for slot in _FG_SUB_SLOTS:
        s = last[f"{slot}_score"]
        sub[slot] = {
            "score":  float(s) if s is not None else None,
            "rating": last[f"{slot}_rating"],
        }
    latest = {
        "score":  float(last["score"]) if last["score"] is not None else None,
        "rating": last["rating"],
        "sub": sub,
    }

    _write({
        "latest_date": last["trade_date"],
        "latest": latest,
        "series": series,
    }, out / "fear_greed.json")


# Roughly one trading year. The 252-day rolling window backs the p20/p50/
# p80 rating thresholds shown on the VIX page; series shown is also capped
# to this length so the chart never spans more than ~1 year.
VIX_WINDOW_DAYS = 252


def _vix_rating(close: float | None, thresholds: dict[str, float | None]) -> str | None:
    """Bucket a VIX close into calm / low / elevated / panic using the
    rolling p20/p50/p80 thresholds. Falls back to None if any threshold is
    missing or close is None."""
    if close is None:
        return None
    p20, p50, p80 = thresholds.get("p20"), thresholds.get("p50"), thresholds.get("p80")
    if p20 is None or p50 is None or p80 is None:
        return None
    if close < p20:
        return "calm"
    if close < p50:
        return "low"
    if close < p80:
        return "elevated"
    return "panic"


def _vix_thresholds(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p20": None, "p50": None, "p80": None}
    import numpy as np
    arr = np.asarray(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return {"p20": None, "p50": None, "p80": None}
    return {
        "p20": round(float(np.percentile(arr, 20)), 2),
        "p50": round(float(np.percentile(arr, 50)), 2),
        "p80": round(float(np.percentile(arr, 80)), 2),
    }


# Roughly three trading years for the chart series. Spread rating uses
# absolute economic thresholds rather than rolling percentile, so the
# window only governs how much history the chart draws — chosen to
# cover the 2022–2024 inversion cycle so users can see the current
# value in its 3-year context.
YIELD_WINDOW_DAYS = 750

# Absolute spread thresholds (percentage points). These mirror the
# canonical newsroom buckets — < 0 = inverted (classic recession lead),
# 0–0.5 = flat, 0.5–1.5 = normal, > 1.5 = steep (expansionary).
YIELD_THRESHOLDS = {"flat": 0.0, "normal": 0.5, "steep": 1.5}


def _yield_rating(spread: float | None) -> str | None:
    if spread is None:
        return None
    if spread < YIELD_THRESHOLDS["flat"]:
        return "inverted"
    if spread < YIELD_THRESHOLDS["normal"]:
        return "flat"
    if spread < YIELD_THRESHOLDS["steep"]:
        return "normal"
    return "steep"


def export_yield_curve(cur, out: Path):
    """US Treasury yield-curve page — last ~1 year of 10Y-2Y and 10Y-3M
    spreads (and the three raw rates) computed from tw.yield_curve."""
    cur.execute(
        f"""
        SELECT trade_date, dgs10, dgs2, dgs3mo
        FROM tw.yield_curve
        WHERE dgs10 IS NOT NULL
          AND (dgs2 IS NOT NULL OR dgs3mo IS NOT NULL)
        ORDER BY trade_date DESC
        LIMIT {YIELD_WINDOW_DAYS}
        """
    )
    rows = list(reversed(cur.fetchall()))

    def _spread(a, b):
        if a is None or b is None:
            return None
        return round(float(a) - float(b), 3)

    series = [
        {
            "date":       r["trade_date"],
            "dgs10":      float(r["dgs10"])  if r["dgs10"]  is not None else None,
            "dgs2":       float(r["dgs2"])   if r["dgs2"]   is not None else None,
            "dgs3mo":     float(r["dgs3mo"]) if r["dgs3mo"] is not None else None,
            "spread_2y":  _spread(r["dgs10"], r["dgs2"]),
            "spread_3m":  _spread(r["dgs10"], r["dgs3mo"]),
        }
        for r in rows
    ]

    latest_2y = next(
        ({"spread": p["spread_2y"], "rating": _yield_rating(p["spread_2y"]), "date": p["date"]}
         for p in reversed(series) if p["spread_2y"] is not None),
        None,
    )
    latest_3m = next(
        ({"spread": p["spread_3m"], "rating": _yield_rating(p["spread_3m"]), "date": p["date"]}
         for p in reversed(series) if p["spread_3m"] is not None),
        None,
    )

    _write({
        "latest_date": series[-1]["date"] if series else None,
        "thresholds":  YIELD_THRESHOLDS,
        "spread_2y":   {"label": "10Y - 2Y", "latest": latest_2y},
        "spread_3m":   {"label": "10Y - 3M", "latest": latest_3m},
        "series":      series,
    }, out / "yield_curve.json")


def export_vix(cur, out: Path):
    """VIX page — last 252 trading days of US ^VIX, TAIFEX TWVIX and Cboe
    VXSMH (semiconductor ETF volatility), each with p20/p50/p80 rating
    thresholds computed over the same window."""

    def _cboe_side(symbol: str):
        """Pull one Cboe series (scrapers.vix_cboe) and derive thresholds."""
        cur.execute(
            f"""
            SELECT trade_date, close
            FROM tw.vix_cboe
            WHERE symbol = %s
            ORDER BY trade_date DESC
            LIMIT {VIX_WINDOW_DAYS}
            """,
            (symbol,),
        )
        rows = list(reversed(cur.fetchall()))
        series = [
            {"date": r["trade_date"], "close": round(float(r["close"]), 2)}
            for r in rows
        ]
        thr = _vix_thresholds([p["close"] for p in series])
        latest = (
            {
                "close":  series[-1]["close"],
                "rating": _vix_rating(series[-1]["close"], thr),
            }
            if series else None
        )
        return series, thr, latest

    # US ^VIX and Cboe VXSMH both come from tw.vix_cboe. VXSMH only has a
    # short history (Cboe publishes a rolling window for the ETF series),
    # so its thresholds start out based on fewer than VIX_WINDOW_DAYS rows
    # and tighten up as the table accumulates.
    us_series, us_thr, us_latest = _cboe_side("VIX")
    vxsmh_series, vxsmh_thr, vxsmh_latest = _cboe_side("VXSMH")

    # TAIFEX TWVIX
    cur.execute(
        f"""
        SELECT trade_date, close, intraday_time
        FROM tw.vix_tw
        ORDER BY trade_date DESC
        LIMIT {VIX_WINDOW_DAYS}
        """
    )
    tw_rows = list(reversed(cur.fetchall()))
    tw_series = [
        {"date": r["trade_date"], "close": round(float(r["close"]), 2)}
        for r in tw_rows
    ]
    tw_thr = _vix_thresholds([p["close"] for p in tw_series])
    # intraday_time: non-NULL HHMMSS = MIS poller wrote this row mid-session.
    # Surface it on the latest payload so the page can render a "盤中 HH:MM" tag.
    tw_latest_intraday = tw_rows[-1]["intraday_time"] if tw_rows else None
    tw_latest = (
        {
            "close":         tw_series[-1]["close"],
            "rating":        _vix_rating(tw_series[-1]["close"], tw_thr),
            "intraday_time": tw_latest_intraday,
        }
        if tw_series else None
    )

    # latest_date is the most recent date that has data on any side; US
    # publishes daily even on TW holidays and vice versa.
    candidates = [s[-1]["date"] for s in (us_series, tw_series, vxsmh_series) if s]
    latest_date = max(candidates) if candidates else None

    _write({
        "latest_date": latest_date,
        "us": {
            "symbol":     "^VIX",
            "label":      "美股 VIX",
            "source":     "Cboe",
            "latest":     us_latest,
            "thresholds": us_thr,
            "series":     us_series,
        },
        "vxsmh": {
            "symbol":     "^VXSMH",
            "label":      "半導體 VXSMH",
            "source":     "Cboe (SMH ETF)",
            "latest":     vxsmh_latest,
            "thresholds": vxsmh_thr,
            "series":     vxsmh_series,
        },
        "tw": {
            "symbol":     "TWVIX",
            "label":      "台指 VIX",
            "source":     "TAIFEX",
            "latest":     tw_latest,
            "thresholds": tw_thr,
            "series":     tw_series,
        },
    }, out / "vix.json")


def export_ftse_taiwan(cur, out: Path):
    """ftse_taiwan.json — 富台指數 (SGX FTSE Taiwan future) 換算的理論 TAIEX。

    Owned by daily_update's update_ftse_taiwan() (and pushed on TW holidays
    too), NOT by export_all — analogous to the intraday JSONs. Latest snapshot
    only; the holiday reference is a single point-in-time number."""
    cur.execute(
        """
        SELECT trade_date, front_contract, ftse_now, ftse_base,
               pct_change, taiex_ref_date, taiex_ref_close,
               theoretical_taiex, captured_at, ftse_bar_date,
               txf_now, txf_pct_change, txf_captured_at
        FROM tw.ftse_taiwan
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    r = cur.fetchone()
    latest = None
    if r:
        latest = {
            "front_contract":    r["front_contract"],
            "ftse_now":          round(float(r["ftse_now"]), 2) if r["ftse_now"] is not None else None,
            "ftse_base":         round(float(r["ftse_base"]), 2) if r["ftse_base"] is not None else None,
            "pct_change":        round(float(r["pct_change"]), 6) if r["pct_change"] is not None else None,
            "taiex_ref_date":    r["taiex_ref_date"],
            "taiex_ref_close":   round(float(r["taiex_ref_close"]), 2) if r["taiex_ref_close"] is not None else None,
            "theoretical_taiex": round(float(r["theoretical_taiex"]), 2) if r["theoretical_taiex"] is not None else None,
            "captured_at":       r["captured_at"],
            # 富台 未開盤: a stored bar date but no theoretical means SGX did not
            # open a new session, so the 富台 leg is null-but-not-a-failure.
            "ftse_bar_date":     r["ftse_bar_date"],
            "ftse_closed":       r["theoretical_taiex"] is None and r["ftse_bar_date"] is not None,
            "txf": {
                "txf_now":         round(float(r["txf_now"]), 2) if r["txf_now"] is not None else None,
                "txf_pct_change":  round(float(r["txf_pct_change"]), 6) if r["txf_pct_change"] is not None else None,
                "txf_captured_at": r["txf_captured_at"],
            },
        }

    _write({
        "latest_date": r["trade_date"] if r else None,
        "latest":      latest,
    }, out / "ftse_taiwan.json")


def export_thermometer(cur, out: Path):
    """thermometer.json — 市場溫度計 (綜合型 L1 descriptive fragility gauge).

    0-100 tension score from 外資期貨定位 + 融資水位. Descriptive, not a crash
    predictor (memory project_market_thermometer). Logic in analysis.market_thermometer."""
    from analysis.market_thermometer import build_thermometer

    _write(build_thermometer(cur), out / "thermometer.json")


def export_theme_calendar(cur, out: Path):
    """theme_calendar.json — 季節題材行事曆 (綜合型 event calendar).

    Recurring time-of-year themes as 12-month bands (股東會軋空/除權息/月營收/財報),
    with a live upcoming-density label for the AGM squeeze. Registry + logic live in
    analysis.theme_calendar."""
    from analysis.theme_calendar import build_calendar

    _write(build_calendar(cur), out / "theme_calendar.json")


def export_cover_squeeze(cur, out: Path):
    """cover_squeeze.json — 股東會強制回補軋空 daily candidate list.

    Heavily-shorted names must force-cover 融券 into a stock's 股東會 book-closure;
    high days-to-cover names squeeze up in the ~6 trading days into the cover date
    (validated edge, memory project_agm_forced_cover_squeeze). Seasonal (Feb–Apr);
    off-season the list is empty by design. Ranking lives in analysis.cover_squeeze
    so the same logic can back a backtest / telegram surface later."""
    from analysis.cover_squeeze import rank_candidates

    snap = rank_candidates(cur)
    _write(snap, out / "cover_squeeze.json")


def export_chip_picks(cur, out: Path):
    """chip_picks.json — 集保大戶選股 (chip_model) 最近 5 週、做多/做空各 top-30。

    Computes signals on the fly from tw.shareholder_distribution (chip_model
    carries its own read-only connections); only the stock-name lookup uses the
    passed cursor. No DB snapshot table — generate_signals already produces every
    week's signals in one pass. market is kept for the frontend TradingView link.
    """
    from chip_model.db_access import load_common_universe
    from chip_model.metrics import compute_metrics
    from chip_model.strategy import Rule, generate_signals

    metrics = compute_metrics()
    universe = load_common_universe()
    rule = Rule(top_n=30)
    longs = generate_signals(metrics, rule, universe, side="long")
    shorts = generate_signals(metrics, rule, universe, side="short")
    if longs.empty:
        _write({"latest_date": None, "weeks": []}, out / "chip_picks.json")
        return

    recent = sorted(longs["data_date"].unique())[-5:]  # oldest -> newest

    ids = set(longs[longs["data_date"].isin(recent)]["stock_id"]) | \
          set(shorts[shorts["data_date"].isin(recent)]["stock_id"])
    cur.execute(
        "SELECT stock_id, name, market FROM tw.stocks WHERE stock_id = ANY(%s)",
        (sorted(ids),),
    )
    info = {r["stock_id"]: (r["name"], r["market"]) for r in cur.fetchall()}

    def rows_for(sig, d):
        wk = sig[sig["data_date"] == d]
        out_rows = []
        for rank, r in enumerate(wk.itertuples(index=False), start=1):
            name, market = info.get(r.stock_id, (None, None))
            out_rows.append({"rank": rank, "ticker": r.stock_id,
                             "name": name, "market": market})
        return out_rows

    weeks = [
        {"date": d, "long": rows_for(longs, d), "short": rows_for(shorts, d)}
        for d in reversed(recent)  # newest first
    ]

    _write({"latest_date": recent[-1], "weeks": weeks}, out / "chip_picks.json")


# 大宗行情 card walls, in page order. Keyed off the `category` each scraper
# stamps on its symbols; a category with no live symbol is dropped.
COMMODITY_CATEGORIES = [
    ("memory",   "記憶體"),
    ("panel",    "面板"),
    ("energy",   "能源"),
    ("petro",    "石化"),
    ("plastics", "塑化"),
    ("rubber",   "橡膠"),
    ("newenergy","新能源材料"),
    ("metal",    "基本金屬"),
    ("precious", "貴金屬"),
    ("steel",    "鋼鐵"),
    ("agri",     "農產"),
    ("fx",       "匯率"),
    ("crypto",   "加密貨幣"),
    ("shipping", "海運"),
]

# Mid/long lookback in DATA POINTS, per series frequency — a point is a day,
# a week or a month depending on the source. Reading 20/60 points back on a
# monthly panel series would reach into 2021; the frontend renders the counts
# alongside the freq's unit ("3月" / "12月").
COMMODITY_LOOKBACK = {
    "daily":   (20, 60),
    "weekly":  (4, 13),
    "monthly": (3, 12),
}

COMMODITY_WINDOW_DAYS = 252  # rows read per symbol — the 52-week lookback


def export_market_quote(cur, out: Path):
    """commodities.json — 大宗行情 page.

    The three scrapers' symbol tables are the single source of truth for both
    the instrument list and its display metadata (name/unit/dp/freq/tv); this
    only reads them. Everything derived — period changes, 52-week range — is
    computed here rather than stored, so a revised upstream close propagates
    without a backfill (see db/migrations/063_add_market_quote.sql).

    No price series is emitted: the cards are numbers only and link out to
    TradingView for the chart, which keeps this file a few KB instead of the
    ~200KB a year of daily closes for every symbol would cost on each push.
    """
    from scrapers.market_hog import HOG_SYMBOLS
    from scrapers.market_html import HTML_SYMBOLS
    from scrapers.market_quote import SYMBOLS

    meta = {**SYMBOLS, **HTML_SYMBOLS, **HOG_SYMBOLS}

    cur.execute(
        """
        SELECT symbol, trade_date, close
        FROM tw.market_quote
        WHERE symbol = ANY(%s) AND close IS NOT NULL
        ORDER BY symbol, trade_date
        """,
        (list(meta),),
    )
    by_symbol: dict[str, list[tuple[date, float]]] = {}
    for r in cur.fetchall():
        by_symbol.setdefault(r["symbol"], []).append(
            (r["trade_date"], float(r["close"]))
        )

    def chg(pts, n):
        """% move over n data points back. A "point" is whatever the series'
        freq is — a week for FBX, a month for the panel quote — which is why
        the frontend labels these off `freq` instead of calling them days."""
        if len(pts) <= n or not pts[-1 - n][1]:
            return None
        return round((pts[-1][1] / pts[-1 - n][1] - 1) * 100, 2)

    quotes = []
    for symbol, m in meta.items():
        pts = by_symbol.get(symbol, [])[-COMMODITY_WINDOW_DAYS:]
        if not pts:
            continue
        n_mid, n_long = COMMODITY_LOOKBACK[m["freq"]]
        latest_date, latest = pts[-1]
        # 52-week range runs off the calendar year up to this symbol's own
        # last print, not the last N points: BDI and the memory spots only
        # accumulate a row per run, so a point-count window would reach back
        # arbitrarily far.
        window = [v for d, v in pts if (latest_date - d).days <= 365]
        hi, lo = max(window), min(window)
        quotes.append({
            "symbol":      symbol,
            "name":        m["name"],
            "category":    m["category"],
            "unit":        m["unit"],
            "dp":          m["dp"],
            "freq":        m["freq"],
            "tv":          m.get("tv"),
            "latest":      latest,
            "latest_date": latest_date,
            "chg_1":       chg(pts, 1),
            "chg_mid":     chg(pts, n_mid),
            "chg_long":    chg(pts, n_long),
            "n_mid":       n_mid,
            "n_long":      n_long,
            "w52_high":    hi,
            "w52_low":     lo,
            "w52_pct":     round((latest - lo) / (hi - lo) * 100, 1) if hi > lo else None,
        })

    # The page's date is the one MOST symbols last printed, not the newest of
    # them: bitcoin trades on Sundays and Yahoo's FX tickers carry a Sunday
    # open tick, so a max() here would drag the page into the weekend and
    # leave every exchange-traded card looking a day or two stale.
    counts = Counter(q["latest_date"] for q in quotes)
    page_date = max(counts, key=lambda d: (counts[d], d)) if counts else None

    _attach_stock_links(cur, quotes, page_date, out)

    live = {q["category"] for q in quotes}
    _write({
        "latest_date": page_date,
        "categories":  [{"key": k, "label": lb}
                        for k, lb in COMMODITY_CATEGORIES if k in live],
        "quotes":      quotes,
    }, out / "commodities.json")


def _attach_stock_links(cur, quotes: list[dict], page_date, out: Path):
    """Hang db.commodity_links on both ends: `stocks` inside each quote for the
    commodity page, and stock_commodities.json for the stock page.

    Names come from tw.stocks rather than the link table so a rename never has
    to be chased through the mapping, and an id that stops existing drops out
    of the page instead of rendering as a dead chip.
    """
    from db.commodity_links import LINKS, ROLE_LABEL, ROLE_SIGN

    ids = sorted({sid for links in LINKS.values() for sid, _, _ in links})
    cur.execute("SELECT stock_id, name FROM tw.stocks WHERE stock_id = ANY(%s)",
                (ids,))
    names = {r["stock_id"]: r["name"] for r in cur.fetchall()}

    by_stock: dict[str, list[dict]] = {}
    for q in quotes:
        q["stocks"] = []
        for sid, role, note in LINKS.get(q["symbol"], []):
            if sid not in names:
                continue
            q["stocks"].append({
                "id": sid, "name": names[sid],
                "role": ROLE_LABEL[role], "sign": ROLE_SIGN[role], "note": note,
            })
            by_stock.setdefault(sid, []).append({
                "symbol": q["symbol"], "name": q["name"], "unit": q["unit"],
                "dp": q["dp"], "latest": q["latest"], "chg_1": q["chg_1"],
                "role": ROLE_LABEL[role], "sign": ROLE_SIGN[role], "note": note,
            })

    _write({"latest_date": page_date, "by_stock": by_stock},
           out / "stock_commodities.json")


def export_all(out_dir: str | None = None):
    if out_dir is None:
        import sys
        if getattr(sys, 'frozen', False):
            base = Path(sys.executable).parent.parent
        else:
            base = Path(__file__).parent.parent
        out_dir = str(base / "frontend" / "public" / "data")

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Exporting to {out}/")

    with get_cursor(commit=False) as cur:
        export_signals(cur, out)
        export_funds(cur, out)
        export_dual_track(cur, out)
        export_flow(cur, out)
        export_hermit(cur, out)
        export_revenue_screens(cur, out)
        export_scores(cur, out)
        export_operations(cur, out)
        export_positions(cur, out)
        export_breadth(cur, out)
        export_margin(cur, out)
        export_insider_pledge(cur, out)
        export_insider_selling(cur, out)
        export_fear_greed(cur, out)
        export_vix(cur, out)
        export_yield_curve(cur, out)
        export_chip_picks(cur, out)
        export_cover_squeeze(cur, out)
        export_thermometer(cur, out)
        export_theme_calendar(cur, out)
        export_market_quote(cur, out)
        # intraday JSONs (scores/operations/positions) are intentionally
        # NOT refreshed here — they are owned by intraday_snapshot.exe
        # which writes them at 12:50 with h(t)-projected bars. Daily
        # close data has no business overwriting that view.

    print("Done.")


if __name__ == "__main__":
    init_db()
    export_all(sys.argv[1] if len(sys.argv) > 1 else None)
