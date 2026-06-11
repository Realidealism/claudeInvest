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
# 2. backtest.json — metrics + trade list
# -----------------------------------------------------------------------

def export_backtest(cur, out: Path):
    cur.execute("""
        SELECT ticker, ticker_name, entry_signal, entry_period,
               entry_date, entry_price, exit_signal, exit_period,
               exit_date, exit_price, return_pct, holding_days
        FROM tw.signal_backtest_results
        ORDER BY entry_date
    """)
    trades = [dict(r) for r in cur.fetchall()]

    # Compute summary metrics
    closed = [t for t in trades if t["return_pct"] is not None]
    returns = [float(t["return_pct"]) for t in closed]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r < 0]

    import numpy as np
    metrics = {}
    if returns:
        metrics = {
            "total_trades": len(closed),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": len(wins) / len(returns),
            "avg_return": float(np.mean(returns)),
            "avg_holding_days": float(np.mean([t["holding_days"] for t in closed if t["holding_days"]])),
            "max_drawdown": float(np.min(
                (lambda eq: eq / np.maximum.accumulate(eq) - 1)(np.cumprod([1 + r for r in returns]))
            )),
        }

    # By entry signal breakdown
    by_entry = {}
    for t in closed:
        sig = t["entry_signal"]
        by_entry.setdefault(sig, []).append(float(t["return_pct"]))
    entry_breakdown = {}
    for sig, rets in by_entry.items():
        w = [r for r in rets if r > 0]
        entry_breakdown[sig] = {
            "trades": len(rets),
            "win_rate": len(w) / len(rets),
            "avg_return": float(np.mean(rets)),
        }

    # Equity curve: cumulative return by exit date (simple sum, trades overlap)
    # Aggregate by date and sort ascending (lightweight-charts requires unique ascending dates)
    daily_returns: dict[str, float] = {}
    for t in closed:
        if t["exit_date"] and t["return_pct"] is not None:
            d = str(t["exit_date"])
            daily_returns[d] = daily_returns.get(d, 0) + float(t["return_pct"])
    equity_curve = []
    cum = 0.0
    for d in sorted(daily_returns):
        cum += daily_returns[d]
        equity_curve.append({"date": d, "value": round(1.0 + cum, 4)})

    # Per-fund performance: join backtest with signals to get fund attribution
    cur.execute("""
        SELECT s.funds, b.return_pct, b.entry_signal, b.ticker
        FROM tw.signal_backtest_results b
        JOIN tw.signals s ON b.ticker = s.ticker
            AND b.entry_signal = s.signal_type
            AND b.entry_period = s.trigger_period
        WHERE b.return_pct IS NOT NULL
    """)
    fund_trades: dict[str, list[float]] = {}
    for r in cur.fetchall():
        ret = float(r["return_pct"])
        for fund_name in r["funds"]:
            fund_trades.setdefault(fund_name, []).append(ret)

    fund_performance = {}
    for fname, rets in sorted(fund_trades.items(), key=lambda x: -len(x[1])):
        w = [r for r in rets if r > 0]
        fund_performance[fname] = {
            "trades": len(rets),
            "win_rate": len(w) / len(rets),
            "avg_return": float(np.mean(rets)),
            "total_return": float(np.sum(rets)),
        }

    _write({
        "metrics": metrics,
        "entry_breakdown": entry_breakdown,
        "trades": trades,
        "equity_curve": equity_curve,
        "fund_performance": fund_performance,
    }, out / "backtest.json")


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
# 5. timeline.json — per-fund holdings across periods
# -----------------------------------------------------------------------

def export_timeline(cur, out: Path):
    cur.execute("""
        SELECT DISTINCT period FROM tw.fund_holdings_monthly ORDER BY period
    """)
    monthly_periods = [r["period"] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT period FROM tw.fund_holdings_quarterly ORDER BY period
    """)
    quarterly_periods = [r["period"] for r in cur.fetchall()]

    # Per fund: ticker trajectory across periods
    cur.execute("SELECT id, code, name FROM tw.funds WHERE fund_type='fund' ORDER BY company")
    funds = cur.fetchall()

    trajectories = {}
    for f in funds:
        cur.execute("""
            SELECT h.period, h.ticker, h.ticker_name, st.market,
                   h.rank, h.weight
            FROM tw.fund_holdings_monthly h
            LEFT JOIN tw.stocks st ON st.stock_id = h.ticker
            WHERE h.fund_id = %s
            ORDER BY h.period, h.rank
        """, (f["id"],))

        by_period = {}
        for r in cur.fetchall():
            by_period.setdefault(r["period"], []).append({
                "ticker": r["ticker"],
                "ticker_name": r["ticker_name"],
                "market": r["market"],
                "rank": r["rank"],
                "weight": float(r["weight"]) if r["weight"] else None,
            })

        trajectories[f["code"]] = {
            "name": f["name"],
            "periods": by_period,
        }

    _write({
        "monthly_periods": monthly_periods,
        "quarterly_periods": quarterly_periods,
        "trajectories": trajectories,
    }, out / "timeline.json")


# -----------------------------------------------------------------------
# 7. dna.json — manager style metrics
# -----------------------------------------------------------------------

def export_dna(cur, out: Path):
    cur.execute("""
        SELECT f.code, f.name, f.company, fm.name AS manager,
               f.fund_type
        FROM tw.funds f
        JOIN tw.fund_managers fm ON f.manager_id = fm.id
        WHERE f.fund_type = 'fund'
        ORDER BY f.company
    """)
    funds = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT period FROM tw.fund_holdings_monthly ORDER BY period
    """)
    periods = [r["period"] for r in cur.fetchall()]

    for f in funds:
        # Concentration: avg weight of top-3 holdings across periods
        cur.execute("""
            SELECT period,
                   SUM(weight) AS top3_weight
            FROM (
                SELECT period, weight,
                       ROW_NUMBER() OVER (PARTITION BY period ORDER BY weight DESC) AS rn
                FROM tw.fund_holdings_monthly m
                JOIN tw.funds fu ON m.fund_id = fu.id
                WHERE fu.code = %s
            ) sub
            WHERE rn <= 3
            GROUP BY period
        """, (f["code"],))
        conc_rows = cur.fetchall()
        f["avg_concentration"] = float(sum(r["top3_weight"] for r in conc_rows) / len(conc_rows)) if conc_rows else 0

        # Turnover: fraction of top-10 that changed between consecutive periods
        cur.execute("""
            SELECT period, ARRAY_AGG(ticker ORDER BY rank) AS tickers
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds fu ON m.fund_id = fu.id
            WHERE fu.code = %s
            GROUP BY period
            ORDER BY period
        """, (f["code"],))
        period_tickers = cur.fetchall()
        turnovers = []
        for i in range(1, len(period_tickers)):
            prev = set(period_tickers[i - 1]["tickers"])
            curr = set(period_tickers[i]["tickers"])
            if prev:
                changed = len(prev.symmetric_difference(curr))
                turnovers.append(changed / max(len(prev), len(curr)))
        f["avg_turnover"] = float(sum(turnovers) / len(turnovers)) if turnovers else 0

    _write({
        "funds": funds,
        "periods": periods,
    }, out / "dna.json")


# -----------------------------------------------------------------------
# 8. flow.json — cross-fund weight changes heatmap
# -----------------------------------------------------------------------

def export_flow(cur, out: Path):
    cur.execute("""
        SELECT DISTINCT period FROM tw.fund_holdings_monthly ORDER BY period
    """)
    periods = [r["period"] for r in cur.fetchall()]
    if len(periods) < 2:
        _write({"periods": periods, "changes": {}}, out / "flow.json")
        return

    latest = periods[-1]
    prev = periods[-2]

    # Get month-end closing prices for share estimation
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

    curr_prices = _month_prices(latest)
    prev_prices = _month_prices(prev)

    # Weight and amount for both periods
    cur.execute("""
        SELECT c.ticker, c.ticker_name, st.market,
               f.code AS fund_code, f.name AS fund_name,
               c.weight AS curr_weight, c.amount AS curr_amount,
               p.weight AS prev_weight, p.amount AS prev_amount
        FROM tw.fund_holdings_monthly c
        JOIN tw.funds f ON c.fund_id = f.id
        LEFT JOIN tw.stocks st ON st.stock_id = c.ticker
        LEFT JOIN tw.fund_holdings_monthly p
            ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
        WHERE c.period = %s
        ORDER BY c.ticker, f.code
    """, (prev, latest))

    changes = {}
    for r in cur.fetchall():
        ticker = r["ticker"]
        if ticker not in changes:
            changes[ticker] = {
                "ticker_name": r["ticker_name"],
                "market": r["market"],
                "funds": {},
            }

        cp = curr_prices.get(ticker)
        pp = prev_prices.get(ticker)
        curr_shares = round(r["curr_amount"] / cp) if r["curr_amount"] and cp else None
        prev_shares = round(r["prev_amount"] / pp) if r["prev_amount"] and pp else None

        if curr_shares is not None and prev_shares is not None:
            share_diff = curr_shares - prev_shares
        elif curr_shares is not None:
            share_diff = curr_shares  # new position
        else:
            share_diff = None

        changes[ticker]["funds"][r["fund_code"]] = {
            "curr": float(r["curr_weight"]) if r["curr_weight"] else None,
            "prev": float(r["prev_weight"]) if r["prev_weight"] else None,
            "diff": share_diff,
        }

    # Fund list for column headers
    cur.execute("SELECT code, name FROM tw.funds WHERE fund_type='fund' ORDER BY company")
    fund_cols = [dict(r) for r in cur.fetchall()]

    _write({
        "periods": [prev, latest],
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
    discover the latest months that have revenue data.

    Tracks first_seen date per (year_month, strategy, stock_id) in a state
    file so each row carries an is_new flag (true iff first_seen == today).
    """
    import importlib

    cur.execute("""
        SELECT DISTINCT year_month FROM tw.monthly_revenue
        ORDER BY year_month DESC LIMIT %s
    """, (n_months,))
    months = [r["year_month"] for r in cur.fetchall()]

    if not months:
        _write({
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "available_months": [],
            "strategies": [],
            "data": {},
        }, out / "revenue_screens.json")
        return

    # Load first_seen state. Schema: { year_month: { strategy_key: { stock_id: date_str } } }.
    state_path = Path(__file__).parent.parent / "data" / "revenue_screens_first_seen.json"
    if state_path.exists():
        with open(state_path, encoding="utf-8") as f:
            first_seen = json.load(f)
    else:
        first_seen = {}

    today_str = date.today().isoformat()

    # Drop months that are no longer in the window so the state file stays bounded.
    for ym in list(first_seen.keys()):
        if ym not in months:
            del first_seen[ym]

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

    data = {}
    for ym in months:
        data[ym] = {}
        ym_state = first_seen.setdefault(ym, {})
        for s in _REVENUE_STRATEGIES:
            mod = importlib.import_module(s["module"])
            try:
                results = mod.screen(ym)
            except Exception as e:
                print(f"  [WARN] {s['key']} {ym} failed: {e}")
                results = []
            keys = [k for (k, _, _) in s["columns"]]
            strat_state = ym_state.setdefault(s["key"], {})

            current_ids = set()
            rows = []
            for r in results:
                stock_id = r.get("stock_id")
                if stock_id is None:
                    continue
                current_ids.add(stock_id)
                if stock_id not in strat_state:
                    strat_state[stock_id] = today_str
                row = {k: r.get(k) for k in keys}
                # Always carry market for the TradingView link, even if not
                # displayed as a column.
                row["market"] = r.get("market")
                row["is_new"] = strat_state[stock_id] == today_str
                rows.append(row)

            # Forget stocks that left the screen — if they come back later they
            # should re-trigger NEW.
            for sid in list(strat_state.keys()):
                if sid not in current_ids:
                    del strat_state[sid]

            data[ym][s["key"]] = rows

    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(first_seen, f, ensure_ascii=False, indent=2)

    _write({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "export_date": today_str,
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
    print("Intraday export done.")


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


# Roughly one trading year for the chart series. Spread rating uses
# absolute economic thresholds rather than rolling percentile, so the
# window only governs how much history the chart draws.
YIELD_WINDOW_DAYS = 252

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
    """VIX page — last 252 trading days of US ^VIX (from CNN F&G's
    volatility sub-indicator) and TAIFEX TWVIX, each with p20/p50/p80
    rating thresholds computed over the same window."""

    # US ^VIX — reuse the value CNN F&G already scrapes daily into
    # cnn_fear_greed.volatility_score (the raw VIX level, not the 0-100
    # normalised rating).
    cur.execute(
        f"""
        SELECT trade_date, volatility_score AS close
        FROM tw.cnn_fear_greed
        WHERE volatility_score IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT {VIX_WINDOW_DAYS}
        """
    )
    us_rows = list(reversed(cur.fetchall()))
    us_series = [
        {"date": r["trade_date"], "close": round(float(r["close"]), 2)}
        for r in us_rows
    ]
    us_thr = _vix_thresholds([p["close"] for p in us_series])
    us_latest = (
        {
            "close":  us_series[-1]["close"],
            "rating": _vix_rating(us_series[-1]["close"], us_thr),
        }
        if us_series else None
    )

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

    # latest_date is the most recent date that has data on either side; US
    # publishes daily even on TW holidays and vice versa.
    candidates = [s[-1]["date"] for s in (us_series, tw_series) if s]
    latest_date = max(candidates) if candidates else None

    _write({
        "latest_date": latest_date,
        "us": {
            "symbol":     "^VIX",
            "label":      "美股 VIX",
            "source":     "CBOE (via CNN F&G)",
            "latest":     us_latest,
            "thresholds": us_thr,
            "series":     us_series,
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
        export_backtest(cur, out)
        export_funds(cur, out)
        export_dual_track(cur, out)
        export_timeline(cur, out)
        export_dna(cur, out)
        export_flow(cur, out)
        export_hermit(cur, out)
        export_revenue_screens(cur, out)
        export_scores(cur, out)
        export_operations(cur, out)
        export_positions(cur, out)
        export_breadth(cur, out)
        export_margin(cur, out)
        export_fear_greed(cur, out)
        export_vix(cur, out)
        export_yield_curve(cur, out)
        # intraday JSONs (scores/operations/positions) are intentionally
        # NOT refreshed here — they are owned by intraday_snapshot.exe
        # which writes them at 12:50 with h(t)-projected bars. Daily
        # close data has no business overwriting that view.

    print("Done.")


if __name__ == "__main__":
    init_db()
    export_all(sys.argv[1] if len(sys.argv) > 1 else None)
