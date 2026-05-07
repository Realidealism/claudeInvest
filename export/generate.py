"""
Generate static JSON files for the frontend from DB data.

Usage:
  python -m export.generate                # output to frontend/public/data/
  python -m export.generate ./out          # output to custom dir
"""

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from db.connection import get_cursor, init_db


def _serial(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _write(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=_serial, indent=2)
    print(f"  {path.name}: {path.stat().st_size:,} bytes")


# -----------------------------------------------------------------------
# 1. signals.json — all signals grouped by type
# -----------------------------------------------------------------------

def export_signals(cur, out: Path):
    cur.execute("""
        SELECT signal_type, ticker, ticker_name, funds,
               trigger_date, trigger_period, weight_change, evidence
        FROM tw.signals
        ORDER BY trigger_period DESC, ticker
    """)
    rows = cur.fetchall()

    by_type = {}
    for r in rows:
        st = r["signal_type"]
        by_type.setdefault(st, []).append({
            "ticker": r["ticker"],
            "ticker_name": r["ticker_name"],
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
                SELECT period, ticker, ticker_name, rank, weight
                FROM tw.fund_holdings_monthly
                WHERE fund_id = %s
                ORDER BY period DESC, rank
            """, (fid,))
            monthly = {}
            for r in cur.fetchall():
                monthly.setdefault(r["period"], []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
                    "rank": r["rank"],
                    "weight": float(r["weight"]) if r["weight"] else None,
                })

            # Quarterly: all periods
            cur.execute("""
                SELECT period, ticker, ticker_name, weight
                FROM tw.fund_holdings_quarterly
                WHERE fund_id = %s
                ORDER BY period DESC, weight DESC
            """, (fid,))
            quarterly = {}
            for r in cur.fetchall():
                quarterly.setdefault(r["period"], []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
                    "weight": float(r["weight"]) if r["weight"] else None,
                })
        else:
            # ETF: use etf_holdings grouped by trade_date
            cur.execute("""
                SELECT trade_date, stock_id AS ticker, stock_name AS ticker_name,
                       weight, shares
                FROM tw.etf_holdings
                WHERE etf_id = %s
                ORDER BY trade_date DESC, weight DESC
            """, (f["code"],))
            monthly = {}
            for r in cur.fetchall():
                key = str(r["trade_date"])
                monthly.setdefault(key, []).append({
                    "ticker": r["ticker"],
                    "ticker_name": r["ticker_name"],
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
# 5. stocks.json — per-ticker cross-fund distribution
# -----------------------------------------------------------------------

def export_stocks(cur, out: Path):
    cur.execute("SELECT MAX(period) FROM tw.fund_holdings_monthly")
    latest_m = list(cur.fetchone().values())[0]

    # All tickers from holdings + signals
    cur.execute("""
        SELECT DISTINCT ticker, ticker_name
        FROM tw.fund_holdings_monthly
        WHERE period = %s
    """, (latest_m,))
    ticker_map = {r["ticker"]: r["ticker_name"] for r in cur.fetchall()}

    # Add tickers from signals that aren't in current holdings
    cur.execute("""
        SELECT DISTINCT s.ticker, s.ticker_name
        FROM tw.signals s
        WHERE s.ticker NOT IN (
            SELECT ticker FROM tw.fund_holdings_monthly WHERE period = %s
        )
    """, (latest_m,))
    for r in cur.fetchall():
        ticker_map[r["ticker"]] = r["ticker_name"]

    tickers = [{"ticker": k, "ticker_name": v} for k, v in sorted(ticker_map.items())]

    # Per-ticker: which funds hold it and with what weight
    stocks = {}
    for t in tickers:
        cur.execute("""
            SELECT f.code, f.name, m.weight, m.rank, m.period
            FROM tw.fund_holdings_monthly m
            JOIN tw.funds f ON m.fund_id = f.id
            WHERE m.ticker = %s
            ORDER BY m.period DESC, m.weight DESC
        """, (t["ticker"],))
        holdings = [dict(r) for r in cur.fetchall()]

        # ETF holdings for this ticker
        cur.execute("""
            SELECT etf_id, weight, trade_date
            FROM tw.etf_holdings
            WHERE stock_id = %s
            ORDER BY trade_date DESC
            LIMIT 7
        """, (t["ticker"],))
        etf = [dict(r) for r in cur.fetchall()]

        # Signals for this ticker
        cur.execute("""
            SELECT signal_type, trigger_period, funds, weight_change
            FROM tw.signals
            WHERE ticker = %s
            ORDER BY trigger_period DESC
        """, (t["ticker"],))
        signals = [dict(r) for r in cur.fetchall()]

        stocks[t["ticker"]] = {
            "ticker_name": t["ticker_name"],
            "fund_holdings": holdings,
            "etf_holdings": etf,
            "signals": signals,
        }

    _write({
        "stocks": stocks,
        "latest_monthly": latest_m,
    }, out / "stocks.json")


# -----------------------------------------------------------------------
# 6. timeline.json — per-fund holdings across periods
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
            SELECT period, ticker, ticker_name, rank, weight
            FROM tw.fund_holdings_monthly
            WHERE fund_id = %s
            ORDER BY period, rank
        """, (f["id"],))

        by_period = {}
        for r in cur.fetchall():
            by_period.setdefault(r["period"], []).append({
                "ticker": r["ticker"],
                "ticker_name": r["ticker_name"],
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
        SELECT c.ticker, c.ticker_name,
               f.code AS fund_code, f.name AS fund_name,
               c.weight AS curr_weight, c.amount AS curr_amount,
               p.weight AS prev_weight, p.amount AS prev_amount
        FROM tw.fund_holdings_monthly c
        JOIN tw.funds f ON c.fund_id = f.id
        LEFT JOIN tw.fund_holdings_monthly p
            ON c.fund_id = p.fund_id AND c.ticker = p.ticker AND p.period = %s
        WHERE c.period = %s
        ORDER BY c.ticker, f.code
    """, (prev, latest))

    changes = {}
    for r in cur.fetchall():
        ticker = r["ticker"]
        if ticker not in changes:
            changes[ticker] = {"ticker_name": r["ticker_name"], "funds": {}}

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
# 9. prices.json — OHLCV for stocks in signals/holdings (last 12 months)
# -----------------------------------------------------------------------

def export_prices(cur, out: Path):
    # Only tickers with recent signals (3 months) or in latest holdings
    cur.execute("""
        SELECT DISTINCT ticker FROM tw.signals
        WHERE trigger_date >= CURRENT_DATE - INTERVAL '6 months'
    """)
    tickers = {r["ticker"] for r in cur.fetchall()}
    cur.execute("""
        SELECT DISTINCT ticker FROM tw.fund_holdings_monthly
        WHERE period = (SELECT MAX(period) FROM tw.fund_holdings_monthly)
    """)
    tickers |= {r["ticker"] for r in cur.fetchall()}

    prices = {}
    for ticker in sorted(tickers):
        cur.execute("""
            SELECT trade_date, open_price, high_price, low_price, close_price, volume
            FROM tw.daily_prices
            WHERE stock_id = %s AND trade_date >= CURRENT_DATE - INTERVAL '12 months'
            ORDER BY trade_date
        """, (ticker,))
        rows = cur.fetchall()
        if not rows:
            continue
        prices[ticker] = [
            {
                "t": str(r["trade_date"]),
                "o": float(r["open_price"]) if r["open_price"] else None,
                "h": float(r["high_price"]) if r["high_price"] else None,
                "l": float(r["low_price"]) if r["low_price"] else None,
                "c": float(r["close_price"]) if r["close_price"] else None,
                "v": int(r["volume"]) if r["volume"] else 0,
            }
            for r in rows
        ]

    _write(prices, out / "prices.json")


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
        SELECT s.rank, s.stock_id, st.name, st.industry, s.score, s.grade,
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
    """Daily ScoreBoard snapshot — top-100 long + top-100 short."""
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

    grouped: dict[str, list] = {sig: [] for sig in SIGNAL_ORDER}
    for r in cur.fetchall():
        sig = r["signal"]
        if sig not in grouped:
            continue  # safety; CHECK constraint should prevent this
        grouped[sig].append({
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
        })

    _write({
        "snapshot_date": latest,
        "signals": grouped,
    }, out / "operations.json")


def export_scores_intraday(cur, out: Path):
    """Intraday (12:50) ScoreBoard preview — top-100 long + top-100 short.

    Mirrors export_scores but reads from tw.score_snapshot_intraday and
    picks the most recent (snapshot_date, snapshot_time) tuple."""
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

    grouped: dict[str, list] = {sig: [] for sig in SIGNAL_ORDER}
    for r in cur.fetchall():
        sig = r["signal"]
        if sig not in grouped:
            continue
        grouped[sig].append({
            "ticker": r["stock_id"],
            "name": r["name"],
            "market": r["market"],
            "turnover": float(r["turnover"]) if r["turnover"] is not None else 0.0,
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
    print("Intraday export done.")


def export_positions(cur, out: Path):
    """Daily snapshot of unified-strategy open positions (long + short)."""
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.open_positions")
    latest = cur.fetchone()["d"]
    if latest is None:
        _write({"snapshot_date": None, "long": [], "short": []},
               out / "positions.json")
        return

    sides = {"long": [], "short": []}
    for side in ("long", "short"):
        cur.execute("""
            SELECT p.stock_id, st.name, st.market,
                   p.entry_date, p.entry_price, p.entry_tier,
                   p.current_close, p.pnl_pct, p.bars_held, p.turnover,
                   p.defense_price, p.defense_reason, p.defense_date
            FROM tw.open_positions p
            JOIN tw.stocks st ON st.stock_id = p.stock_id
            WHERE p.snapshot_date = %s AND p.side = %s
            ORDER BY p.turnover DESC NULLS LAST, p.stock_id
        """, (latest, side))
        for r in cur.fetchall():
            sides[side].append({
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
            })

    _write({
        "snapshot_date": latest,
        "long": sides["long"],
        "short": sides["short"],
    }, out / "positions.json")


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
        export_stocks(cur, out)
        export_timeline(cur, out)
        export_dna(cur, out)
        export_flow(cur, out)
        export_prices(cur, out)
        export_hermit(cur, out)
        export_revenue_screens(cur, out)
        export_scores(cur, out)
        export_operations(cur, out)
        export_positions(cur, out)
        # intraday JSONs (scores/operations/positions) are intentionally
        # NOT refreshed here — they are owned by intraday_snapshot.exe
        # which writes them at 12:50 with h(t)-projected bars. Daily
        # close data has no business overwriting that view.

    print("Done.")


if __name__ == "__main__":
    init_db()
    export_all(sys.argv[1] if len(sys.argv) > 1 else None)
