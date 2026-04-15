"""
Quarterly financial statement scraper — data source: FinMind API.

FinMind wraps MOPS (公開資訊觀測站) data and returns structured JSON.
No token required for basic queries; free tier allows ~600 queries/hour.

Endpoints used:
  - TaiwanStockFinancialStatements  → income statement (17 fields, summary)
  - TaiwanStockBalanceSheet         → balance sheet (detailed, 100+ fields)
  - TaiwanStockCashFlowsStatement   → cash flow (30+ fields)

IMPORTANT notes on the data:
  * FinMind values are in full NTD (not thousands). We divide by 1000 on insert
    to stay consistent with tw.monthly_revenue which uses 千元.
  * FinMind normalises income statement to SINGLE-QUARTER values (period_type='Q').
    Cash flow is returned as YTD CUMULATIVE (period_type='A'); single-quarter
    values are derived post-hoc by subtracting the prior quarter.
  * Balance sheet is a point-in-time snapshot, no period_type distinction.
  * This scraper currently targets 一般業 (general industry). Banks, insurance
    and securities firms have different IFRS account structures and will be
    added in a later phase.
"""

import time
from datetime import date

import requests

from db.connection import get_cursor


FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
# FinMind free tier: ~600 req/hour. Be conservative: one request every 2s.
MIN_INTERVAL = 2.0
MAX_RETRIES = 3
BACKOFF_BASE = 10.0

_last_req_time = 0.0


# ---------- FinMind type → DB column mapping (general industry only) ----------

INCOME_MAP = {
    "Revenue":                "revenue",
    "CostOfGoodsSold":        "cost_of_revenue",
    "GrossProfit":            "gross_profit",
    "OperatingExpenses":      "operating_expenses",
    "OperatingIncome":        "operating_income",
    "TotalNonoperatingIncomeAndExpense": "non_operating_income",
    "PreTaxIncome":           "pretax_income",
    "TAX":                    "tax_expense",
    "IncomeAfterTaxes":       "net_income",
    "EquityAttributableToOwnersOfParent": "net_income_attributable",
    "EPS":                    "eps",
}

BALANCE_MAP = {
    "CashAndCashEquivalents":       "cash_and_equivalents",
    "AccountsReceivableNet":        "accounts_receivable",
    "Inventories":                  "inventory",
    "OtherCurrentAssets":           "other_current_assets",
    "CurrentAssets":                "current_assets",
    "PropertyPlantAndEquipment":    "ppe",
    "IntangibleAssets":             "intangible_assets",
    "OtherNoncurrentAssets":        "other_assets",
    "TotalAssets":                  "total_assets",
    "ShorttermBorrowings":          "short_term_debt",
    "AccountsPayable":              "accounts_payable",
    "OtherCurrentLiabilities":      "other_current_liab",
    "CurrentLiabilities":           "current_liabilities",
    "LongtermBorrowings":           "long_term_debt",
    "OtherNoncurrentLiabilities":   "other_liabilities",
    "Liabilities":                  "total_liabilities",
    "CapitalStock":                 "common_stock",
    "CapitalSurplus":               "capital_surplus",
    "RetainedEarnings":             "retained_earnings",
    "Equity":                       "total_equity",
    "EquityAttributableToOwnersOfParent": "equity_attributable",
    "NoncontrollingInterests":      "minority_interest",
}

CASHFLOW_MAP = {
    "NetIncomeBeforeTax":                        "net_income_cf",
    "Depreciation":                              "depreciation",
    "AmortizationExpense":                       "amortization",
    "CashFlowsFromOperatingActivities":          "operating_cash_flow",
    "NetCashInflowFromOperatingActivities":      "operating_cash_flow",  # alt key
    "PropertyAndPlantAndEquipment":              "capex",
    "CashProvidedByInvestingActivities":         "investing_cash_flow",
    "CashFlowsProvidedFromFinancingActivities":  "financing_cash_flow",
    "CashBalancesIncrease":                      "net_change_in_cash",
}


# ---------- HTTP ----------

def _rate_limit():
    global _last_req_time
    elapsed = time.time() - _last_req_time
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_req_time = time.time()


def _fetch(dataset: str, stock_id: str, start: str, end: str) -> list[dict] | None:
    """Fetch one FinMind dataset for one stock over a date range."""
    params = {
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start,
        "end_date": end,
    }
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            r = requests.get(FINMIND_URL, params=params, timeout=30)
            if r.status_code == 402:
                # Rate limit exceeded on free tier
                wait = BACKOFF_BASE * (2 ** attempt)
                print(f"  [402] FinMind rate limit hit, waiting {wait}s ...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            d = r.json()
            if d.get("msg") != "success":
                print(f"  FinMind error: {d.get('msg')}")
                return None
            return d.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f"  Request error (attempt {attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE)
    return None


# ---------- Parsing ----------

def _group_by_period(rows: list[dict], field_map: dict) -> dict:
    """
    Group FinMind rows by report date and project to DB columns.
    Returns {date_str: {column: value_in_thousands}}.
    FinMind returns values in NTD; we convert to 千元 on the fly.
    """
    result: dict[str, dict] = {}
    for row in rows:
        t = row.get("type")
        if t not in field_map:
            continue
        col = field_map[t]
        d = row["date"]
        v = row.get("value")
        if v is None:
            continue
        # EPS stays as-is (NTD per share); other fields convert to 千元
        if col == "eps":
            val = round(float(v), 2)
        else:
            val = int(float(v) / 1000)
        # Don't overwrite if multiple FinMind types map to same column (e.g. OCF alt keys)
        result.setdefault(d, {}).setdefault(col, val)
    return result


def _date_to_quarter(date_str: str) -> tuple[int, int] | None:
    """Convert report date (YYYY-MM-DD) to (year, quarter). Returns None if invalid."""
    y, m, _ = date_str.split("-")
    y, m = int(y), int(m)
    q_map = {3: 1, 6: 2, 9: 3, 12: 4}
    q = q_map.get(m)
    return (y, q) if q else None


# ---------- Save ----------

def _upsert(table: str, pk_cols: list[str], row: dict):
    cols = list(row.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols)
    pk_list = ", ".join(pk_cols)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk_list}) DO UPDATE SET {updates}, updated_at=NOW()"
    )
    with get_cursor() as cur:
        cur.execute(sql, [row[c] for c in cols])


def _save_income(stock_id: str, grouped: dict):
    for dstr, fields in grouped.items():
        yq = _date_to_quarter(dstr)
        if not yq:
            continue
        row = {"stock_id": stock_id, "year": yq[0], "quarter": yq[1],
               "period_type": "Q", **fields}
        _upsert("tw.income_statements", ["stock_id", "year", "quarter", "period_type"], row)


def _save_balance(stock_id: str, grouped: dict):
    for dstr, fields in grouped.items():
        yq = _date_to_quarter(dstr)
        if not yq:
            continue
        # Derive book_value_per_share if we have equity + common_stock
        # Each share has par value 10 NTD, so shares = common_stock (千元) * 1000 / 10
        if "equity_attributable" in fields and "common_stock" in fields and fields["common_stock"]:
            shares = fields["common_stock"] * 1000 // 10   # in shares
            fields["shares_outstanding"] = shares
            if shares:
                fields["book_value_per_share"] = round(
                    fields["equity_attributable"] * 1000 / shares, 2
                )
        row = {"stock_id": stock_id, "year": yq[0], "quarter": yq[1], **fields}
        _upsert("tw.balance_sheets", ["stock_id", "year", "quarter"], row)


def _save_cashflow(stock_id: str, grouped: dict):
    for dstr, fields in grouped.items():
        yq = _date_to_quarter(dstr)
        if not yq:
            continue
        # FCF = OCF + CAPEX (CAPEX already negative in FinMind)
        ocf = fields.get("operating_cash_flow")
        capex = fields.get("capex")
        if ocf is not None and capex is not None:
            fields["free_cash_flow"] = ocf + capex
        row = {"stock_id": stock_id, "year": yq[0], "quarter": yq[1],
               "period_type": "A", **fields}
        _upsert("tw.cash_flows", ["stock_id", "year", "quarter", "period_type"], row)


# ---------- Public API ----------

def scrape_stock(stock_id: str, start_year: int = 2013, end_year: int | None = None):
    """Fetch and save all three statements for one stock across a year range."""
    if end_year is None:
        end_year = date.today().year
    start = f"{start_year}-01-01"
    end = f"{end_year}-12-31"
    print(f"[{stock_id}] fetching {start} ~ {end}")

    income = _fetch("TaiwanStockFinancialStatements", stock_id, start, end)
    if income:
        _save_income(stock_id, _group_by_period(income, INCOME_MAP))
        print(f"  income: {len(income)} rows")

    balance = _fetch("TaiwanStockBalanceSheet", stock_id, start, end)
    if balance:
        _save_balance(stock_id, _group_by_period(balance, BALANCE_MAP))
        print(f"  balance: {len(balance)} rows")

    cashflow = _fetch("TaiwanStockCashFlowsStatement", stock_id, start, end)
    if cashflow:
        _save_cashflow(stock_id, _group_by_period(cashflow, CASHFLOW_MAP))
        print(f"  cashflow: {len(cashflow)} rows")


def _already_covered(stock_id: str, start_year: int, end_year: int) -> bool:
    """Skip if we already have at least one income-statement row per year in range."""
    expected = end_year - start_year + 1
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT COUNT(DISTINCT year) FROM tw.income_statements "
            "WHERE stock_id = %s AND year BETWEEN %s AND %s",
            (stock_id, start_year, end_year),
        )
        got = cur.fetchone()["count"]
    return got >= expected


def scrape_all(start_year: int = 2013, end_year: int | None = None,
               market_filter: tuple[str, ...] = ("TWSE", "TPEx"),
               resume: bool = True):
    """Iterate all active stocks in tw.stocks and fetch financials.
    With resume=True, skip stocks that already have data for all years in range.
    """
    if end_year is None:
        end_year = date.today().year
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT stock_id FROM tw.stocks "
            "WHERE is_active = TRUE AND market = ANY(%s) "
            "AND security_type = 'STOCK' "
            "ORDER BY stock_id",
            (list(market_filter),),
        )
        stock_ids = [r["stock_id"] for r in cur.fetchall()]

    print(f"Scraping financials for {len(stock_ids)} stocks ({start_year}~{end_year})")
    skipped = 0
    for i, sid in enumerate(stock_ids, 1):
        if resume and _already_covered(sid, start_year, end_year):
            skipped += 1
            continue
        print(f"[{i}/{len(stock_ids)}] {sid}  (skipped so far: {skipped})")
        try:
            scrape_stock(sid, start_year, end_year)
        except Exception as e:
            print(f"  ERROR on {sid}: {e}")
    print(f"\nDone. Skipped {skipped} already-covered stocks.")


if __name__ == "__main__":
    import sys
    # Full-market backfill: python -m scrapers.financials all [start_year] [end_year]
    # Single stock:        python -m scrapers.financials 2330 [start_year] [end_year]
    if len(sys.argv) >= 2 and sys.argv[1] == "all":
        sy = int(sys.argv[2]) if len(sys.argv) >= 3 else 2013
        ey = int(sys.argv[3]) if len(sys.argv) >= 4 else None
        scrape_all(sy, ey)
    elif len(sys.argv) >= 2:
        sid = sys.argv[1]
        sy = int(sys.argv[2]) if len(sys.argv) >= 3 else 2013
        ey = int(sys.argv[3]) if len(sys.argv) >= 4 else None
        scrape_stock(sid, sy, ey)
    else:
        scrape_stock("2330", 2023, 2023)
