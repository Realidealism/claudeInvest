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
            "max_drawdown": float(np.min(np.cumprod([1 + r for r in returns]) - np.maximum.accumulate(np.cumprod([1 + r for r in returns])))),
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

    _write({
        "metrics": metrics,
        "entry_breakdown": entry_breakdown,
        "trades": trades,
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

    # All tickers that appear in any holdings
    cur.execute("""
        SELECT DISTINCT ticker, ticker_name
        FROM tw.fund_holdings_monthly
        WHERE period = %s
        ORDER BY ticker
    """, (latest_m,))
    tickers = [dict(r) for r in cur.fetchall()]

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

    # Weight changes between last two periods for all fund-ticker combos
    cur.execute("""
        SELECT c.ticker, c.ticker_name,
               f.code AS fund_code, f.name AS fund_name,
               c.weight AS curr_weight,
               p.weight AS prev_weight,
               c.weight - COALESCE(p.weight, 0) AS weight_diff
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
        changes[ticker]["funds"][r["fund_code"]] = {
            "curr": float(r["curr_weight"]) if r["curr_weight"] else None,
            "prev": float(r["prev_weight"]) if r["prev_weight"] else None,
            "diff": float(r["weight_diff"]) if r["weight_diff"] else None,
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

def export_all(out_dir: str | None = None):
    if out_dir is None:
        out_dir = str(Path(__file__).parent.parent / "frontend" / "public" / "data")

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

    print("Done.")


if __name__ == "__main__":
    init_db()
    export_all(sys.argv[1] if len(sys.argv) > 1 else None)
