"""
ETF holdings tracker.

Scrapes daily holdings for selected active ETFs and computes diff
against the previous trading day.

Tracked ETFs:
  00981A  主動統一台股增長      (ezmoney, fundCode=49YTW)
  00988A  主動統一全球創新      (ezmoney, fundCode=61YTW)
  00992A  群益台灣科技創新主動  (capitalfund, fundId=500)
"""

import json
import re
import html as html_mod
from datetime import date

from db.connection import get_cursor
from utils.http_client import fetch, get_session, _get_domain, _wait_for_rate_limit

# ---------------------------------------------------------------------------
# ETF registry
# ---------------------------------------------------------------------------

ETF_REGISTRY = [
    {
        "etf_id": "00981A",
        "source": "ezmoney",
        "fund_code": "49YTW",
    },
    {
        "etf_id": "00988A",
        "source": "ezmoney",
        "fund_code": "61YTW",
    },
    {
        "etf_id": "00992A",
        "source": "capitalfund",
        "fund_id": "500",
    },
]

# ---------------------------------------------------------------------------
# Ezmoney (統一投信) — holdings embedded in HTML data-content
# ---------------------------------------------------------------------------

EZMONEY_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info"


def _fetch_ezmoney(fund_code: str) -> list[dict]:
    """
    Fetch holdings from ezmoney HTML page.
    Returns list of {stock_id, stock_name, shares, weight}.
    """
    resp = fetch(EZMONEY_URL, params={"fundCode": fund_code})
    if resp is None:
        print(f"  [ERROR] Failed to fetch ezmoney page for {fund_code}")
        return []

    page = resp.text

    # Extract DataAsset JSON from data-content attribute
    m = re.search(r'id="DataAsset"\s+data-content="([^"]+)"', page)
    if not m:
        print(f"  [ERROR] DataAsset not found in page for {fund_code}")
        return []

    raw = html_mod.unescape(m.group(1))
    assets = json.loads(raw)

    # Find stock (ST) asset type
    holdings = []
    for asset in assets:
        if asset.get("AssetCode") != "ST":
            continue
        details = asset.get("Details") or []
        for d in details:
            stock_id = (d.get("DetailCode") or "").strip()
            stock_name = (d.get("DetailName") or "").strip()
            shares = d.get("Share")
            weight = d.get("NavRate")
            if stock_id and shares is not None:
                holdings.append({
                    "stock_id": stock_id,
                    "stock_name": stock_name,
                    "shares": int(shares),
                    "weight": float(weight) if weight is not None else None,
                })

    return holdings

# ---------------------------------------------------------------------------
# Capitalfund (群益投信) — JSON API
# ---------------------------------------------------------------------------

CAPITALFUND_URL = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"


def _fetch_capitalfund(fund_id: str) -> list[dict]:
    """
    Fetch holdings from capitalfund API (POST with JSON body).
    Returns list of {stock_id, stock_name, shares, weight}.
    """
    domain = _get_domain(CAPITALFUND_URL)
    session = get_session()
    _wait_for_rate_limit(domain)
    try:
        resp = session.post(
            CAPITALFUND_URL,
            json={"fundId": fund_id},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] Failed to fetch capitalfund API for fundId={fund_id}: {e}")
        return []

    stocks = data.get("data", data).get("stocks", [])
    holdings = []
    for s in stocks:
        stock_id = (s.get("stocNo") or "").strip()
        stock_name = (s.get("stocName") or "").strip()
        shares = s.get("share")
        weight = s.get("weight")
        if stock_id and shares is not None:
            holdings.append({
                "stock_id": stock_id,
                "stock_name": stock_name,
                "shares": int(shares),
                "weight": float(weight) if weight is not None else None,
            })

    return holdings

# ---------------------------------------------------------------------------
# Fetch dispatcher
# ---------------------------------------------------------------------------


def _fetch_holdings(etf: dict) -> list[dict]:
    source = etf["source"]
    if source == "ezmoney":
        return _fetch_ezmoney(etf["fund_code"])
    elif source == "capitalfund":
        return _fetch_capitalfund(etf["fund_id"])
    else:
        print(f"  [ERROR] Unknown source: {source}")
        return []

# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------


def _compute_diff(prev: list[dict], curr: list[dict]) -> list[dict]:
    """Compare two holdings snapshots, return list of changes."""
    prev_map = {h["stock_id"]: h for h in prev}
    curr_map = {h["stock_id"]: h for h in curr}
    all_ids = set(prev_map) | set(curr_map)

    diffs = []
    for sid in all_ids:
        p = prev_map.get(sid)
        c = curr_map.get(sid)

        if p and not c:
            diffs.append({
                "stock_id": sid,
                "stock_name": p["stock_name"],
                "change_type": "removed",
                "prev_shares": p["shares"],
                "curr_shares": None,
                "share_diff": None,
                "prev_weight": p["weight"],
                "curr_weight": None,
                "weight_diff": None,
            })
        elif c and not p:
            diffs.append({
                "stock_id": sid,
                "stock_name": c["stock_name"],
                "change_type": "added",
                "prev_shares": None,
                "curr_shares": c["shares"],
                "share_diff": None,
                "prev_weight": None,
                "curr_weight": c["weight"],
                "weight_diff": None,
            })
        elif c["shares"] != p["shares"] or c["weight"] != p["weight"]:
            share_diff = c["shares"] - p["shares"]
            w_diff = None
            if c["weight"] is not None and p["weight"] is not None:
                w_diff = round(c["weight"] - p["weight"], 4)
            change_type = "increased" if share_diff > 0 else "decreased"
            diffs.append({
                "stock_id": sid,
                "stock_name": c["stock_name"],
                "change_type": change_type,
                "prev_shares": p["shares"],
                "curr_shares": c["shares"],
                "share_diff": share_diff,
                "prev_weight": p["weight"],
                "curr_weight": c["weight"],
                "weight_diff": w_diff,
            })

    return diffs

# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------


def _get_prev_holdings(cur, etf_id: str, trade_date: date) -> list[dict]:
    """Get the most recent holdings before trade_date."""
    cur.execute("""
        SELECT stock_id, stock_name, shares, weight
        FROM tw.etf_holdings
        WHERE etf_id = %s AND trade_date < %s
        ORDER BY trade_date DESC
        LIMIT 200
    """, (etf_id, trade_date))
    rows = cur.fetchall()
    if not rows:
        return []
    # All rows share the same trade_date (the most recent one)
    target_date = rows[0]["trade_date"] if rows else None
    return [
        {
            "stock_id": r["stock_id"],
            "stock_name": r["stock_name"],
            "shares": r["shares"],
            "weight": float(r["weight"]) if r["weight"] is not None else None,
        }
        for r in rows if r["trade_date"] == target_date
    ]


def _save_holdings(cur, etf_id: str, trade_date: date, holdings: list[dict]):
    for h in holdings:
        cur.execute("""
            INSERT INTO tw.etf_holdings (etf_id, trade_date, stock_id, stock_name, shares, weight)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_id, trade_date, stock_id) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                shares = EXCLUDED.shares,
                weight = EXCLUDED.weight
        """, (etf_id, trade_date, h["stock_id"], h["stock_name"], h["shares"], h["weight"]))


def _save_diff(cur, etf_id: str, trade_date: date, diffs: list[dict]):
    for d in diffs:
        cur.execute("""
            INSERT INTO tw.etf_holdings_diff
                (etf_id, trade_date, stock_id, stock_name, change_type,
                 prev_shares, curr_shares, share_diff, prev_weight, curr_weight, weight_diff)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_id, trade_date, stock_id) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                change_type = EXCLUDED.change_type,
                prev_shares = EXCLUDED.prev_shares,
                curr_shares = EXCLUDED.curr_shares,
                share_diff = EXCLUDED.share_diff,
                prev_weight = EXCLUDED.prev_weight,
                curr_weight = EXCLUDED.curr_weight,
                weight_diff = EXCLUDED.weight_diff
        """, (
            etf_id, trade_date, d["stock_id"], d["stock_name"], d["change_type"],
            d["prev_shares"], d["curr_shares"], d["share_diff"],
            d["prev_weight"], d["curr_weight"], d["weight_diff"],
        ))

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def scrape_date(trade_date: date):
    """Scrape ETF holdings for all tracked ETFs and compute diffs."""
    for etf in ETF_REGISTRY:
        etf_id = etf["etf_id"]
        print(f"  Fetching holdings for {etf_id} ...")

        holdings = _fetch_holdings(etf)
        if not holdings:
            print(f"  [WARN] No holdings data for {etf_id}, skipping.")
            continue

        print(f"  {etf_id}: {len(holdings)} stocks")

        with get_cursor() as cur:
            prev = _get_prev_holdings(cur, etf_id, trade_date)
            _save_holdings(cur, etf_id, trade_date, holdings)

            if prev:
                diffs = _compute_diff(prev, holdings)
                _save_diff(cur, etf_id, trade_date, diffs)

                added = sum(1 for d in diffs if d["change_type"] == "added")
                removed = sum(1 for d in diffs if d["change_type"] == "removed")
                changed = len(diffs) - added - removed
                print(f"  {etf_id} diff: +{added} added, -{removed} removed, ~{changed} changed")
            else:
                print(f"  {etf_id}: first snapshot, no diff computed.")
