"""
SITCA fund holdings scraper.

Scrapes monthly (IN2629) and quarterly (IN2630) fund holdings reports
from the Securities Investment Trust & Consulting Association (SITCA).

Monthly: Top 10 holdings per fund
Quarterly: All holdings >= 1% NAV
"""

import re
from datetime import date

import requests
from bs4 import BeautifulSoup

from db.connection import get_cursor

# ---------------------------------------------------------------------------
# SITCA company codes → our fund codes
# ---------------------------------------------------------------------------

# Map: SITCA company code → list of (fund_code_in_our_db, name_keywords)
# name_keywords are used to match the fund name in SITCA response
FUND_REGISTRY = {
    "A0009": [  # 統一投信
        ("unitec-allweather",  ["統一全天候基金"]),
        ("unitec-gallop",      ["統一奔騰基金"]),
        ("unitec-darkhorse",   ["統一黑馬基金"]),
        ("unitec-smid",        ["統一中小基金"]),
        ("unitec-gc-smid",     ["統一大中華中小基金"]),
    ],
    "A0022": [  # 復華投信
        ("fhtrust-highgrowth", ["復華高成長基金"]),
        ("fhtrust-allround",   ["復華全方位基金"]),
    ],
    "A0032": [  # 野村投信
        ("nomura-quality",     ["野村優質基金"]),
        ("nomura-hightech",    ["野村高科技基金"]),
    ],
    "A0036": [  # 安聯投信
        ("allianz-dam",        ["安聯台灣大壩基金"]),
        ("allianz-tech",       ["安聯台灣科技基金"]),
    ],
    "A0047": [  # 台新投信
        ("taishin-mainstream", ["台新主流基金"]),
    ],
    "A0005": [  # 元大投信
        ("yuanta-newmain",     ["元大新主流基金"]),
    ],
}

SITCA_URL = "https://www.sitca.org.tw/ROC/Industry/IN2629.aspx?PGMID=IN0202"
SITCA_Q_URL = "https://www.sitca.org.tw/ROC/Industry/IN2630.aspx?PGMID=IN0203"

# ---------------------------------------------------------------------------
# Form helpers
# ---------------------------------------------------------------------------


def _get_form_fields(session: requests.Session, url: str) -> dict:
    """GET the page and collect all form fields."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    resp = session.get(url, timeout=30, headers=headers)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    form_data = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = inp.get("type", "text")
        val = inp.get("value", "")
        if itype == "radio":
            if inp.get("checked") is not None:
                form_data[name] = val
        elif itype not in ("button", "submit"):
            form_data[name] = val

    for sel in soup.find_all("select"):
        name = sel.get("name")
        if name:
            selected = sel.find("option", selected=True)
            form_data[name] = selected.get("value", "") if selected else ""

    return form_data


def _post_query(session: requests.Session, url: str,
                form_data: dict, company_code: str, period: str) -> str:
    """Submit query and return response HTML."""
    form_data["ctl00$ContentPlaceHolder1$rdo1"] = "rbComid"
    form_data["ctl00$ContentPlaceHolder1$ddlQ_YM"] = period
    form_data["ctl00$ContentPlaceHolder1$ddlQ_Comid"] = company_code
    form_data["ctl00$ContentPlaceHolder1$BtnQuery"] = "查詢"
    form_data.pop("ctl00$ContentPlaceHolder1$BtnExport", None)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": url,
    }
    resp = session.post(url, data=form_data, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.text


# ---------------------------------------------------------------------------
# HTML table parsing
# ---------------------------------------------------------------------------


def _match_fund(fund_name_raw: str, fund_list: list) -> str | None:
    """Match a raw SITCA fund name to our fund code."""
    for fund_code, keywords in fund_list:
        for kw in keywords:
            if kw in fund_name_raw:
                return fund_code
    return None


def _parse_monthly_holdings(html: str, fund_list: list) -> dict[str, list[dict]]:
    """
    Parse IN2629 monthly response HTML.

    Returns: {fund_code: [{ticker, ticker_name, rank, weight}, ...]}
    """
    soup = BeautifulSoup(html, "html.parser")

    # The main data table is the largest table with many rows
    tables = soup.find_all("table")
    data_table = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
    if not data_table:
        return {}

    # Flatten all td text from the data table
    cells = []
    for td in data_table.find_all("td"):
        text = td.get_text(strip=True)
        cells.append(text)

    # Parse the flat cell list into fund holdings
    # Pattern: fund_name, then repeating [rank, type, code, name, amount, guarantor,
    #          subordinated, units, weight], then "合計", total_weight
    result = {}
    i = 0
    current_fund = None
    current_holdings = []

    while i < len(cells):
        cell = cells[i]

        # Check if this is a "合計" summary row
        if cell == "合計" and current_fund:
            # Save current fund's holdings
            fund_code = _match_fund(current_fund, fund_list)
            if fund_code and current_holdings:
                result[fund_code] = current_holdings
            current_fund = None
            current_holdings = []
            i += 2  # skip "合計" and total weight
            continue

        # Check if this is a fund name (long text, not a number, not a type)
        if (len(cell) > 4 and not cell.isdigit()
                and not cell.startswith("國內") and not cell.startswith("國外")
                and "公司債" not in cell and "政府公債" not in cell
                and "基金" in cell):
            current_fund = cell
            current_holdings = []
            i += 1
            continue

        # Check if this is a rank number (1-10)
        if cell.isdigit() and 1 <= int(cell) <= 10 and current_fund:
            rank = int(cell)
            # Next cells: type, code, name, amount, guarantor, subordinated, units, weight
            if i + 8 < len(cells):
                stock_type = cells[i + 1]  # 國內上市, 國內上櫃, etc.
                ticker = cells[i + 2]
                ticker_name = cells[i + 3]
                weight_str = cells[i + 8]

                # Only keep domestic stocks (4-digit code)
                is_domestic = ticker.isdigit() and len(ticker) == 4
                try:
                    weight = float(weight_str.replace(",", ""))
                except (ValueError, TypeError):
                    weight = None

                current_holdings.append({
                    "ticker": ticker,
                    "ticker_name": ticker_name,
                    "rank": rank,
                    "weight": weight,
                    "stock_type": stock_type,
                    "is_domestic": is_domestic,
                })
                i += 9  # skip all cells in this row
                continue

        i += 1

    return result


def _parse_quarterly_holdings(html: str, fund_list: list) -> dict[str, list[dict]]:
    """
    Parse IN2630 quarterly response HTML.

    Returns: {fund_code: [{ticker, ticker_name, weight}, ...]}
    """
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    data_table = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
    if not data_table:
        return {}

    cells = []
    for td in data_table.find_all("td"):
        text = td.get_text(strip=True)
        cells.append(text)

    # Quarterly has no rank column.
    # Each holding: [type, code, name, amount, guarantor, subordinated, units, weight]
    result = {}
    i = 0
    current_fund = None
    current_holdings = []

    while i < len(cells):
        cell = cells[i]

        if cell == "合計" and current_fund:
            fund_code = _match_fund(current_fund, fund_list)
            if fund_code and current_holdings:
                result[fund_code] = current_holdings
            current_fund = None
            current_holdings = []
            i += 2
            continue

        if (len(cell) > 4 and not cell.isdigit()
                and not cell.startswith("國內") and not cell.startswith("國外")
                and "公司債" not in cell and "政府公債" not in cell
                and "基金" in cell):
            current_fund = cell
            current_holdings = []
            i += 1
            continue

        # Quarterly: type, code, name, amount, guarantor, subordinated, units, weight
        # Match all known SITCA type prefixes to advance 8 cells correctly
        if current_fund and (cell.startswith("國內") or cell.startswith("國外")
                             or "公司債" in cell or "政府公債" in cell
                             or cell.startswith("轉換") or cell.startswith("投信")
                             or cell.startswith("依境外") or cell.startswith("同公司")):
            if i + 7 < len(cells):
                stock_type = cell
                ticker = cells[i + 1]
                ticker_name = cells[i + 2]
                weight_str = cells[i + 7]

                is_domestic = ticker.isdigit() and len(ticker) == 4
                try:
                    weight = float(weight_str.replace(",", ""))
                except (ValueError, TypeError):
                    weight = None

                if weight is not None and weight >= 1.0:
                    current_holdings.append({
                        "ticker": ticker,
                        "ticker_name": ticker_name,
                        "weight": weight,
                        "stock_type": stock_type,
                        "is_domestic": is_domestic,
                    })
                i += 8
                continue

        i += 1

    return result


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------


def _get_fund_id_map(cur) -> dict[str, int]:
    """Get mapping of fund code → fund DB id."""
    cur.execute("SELECT id, code FROM tw.funds")
    return {r["code"]: r["id"] for r in cur.fetchall()}


def _save_monthly(cur, fund_db_id: int, period: str, holdings: list[dict]):
    for h in holdings:
        if not h.get("is_domestic"):
            continue
        cur.execute("""
            INSERT INTO tw.fund_holdings_monthly
                (fund_id, period, ticker, ticker_name, rank, weight)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fund_id, period, ticker) DO UPDATE SET
                ticker_name = EXCLUDED.ticker_name,
                rank = EXCLUDED.rank,
                weight = EXCLUDED.weight
        """, (fund_db_id, period, h["ticker"], h["ticker_name"],
              h.get("rank"), h.get("weight")))


def _save_quarterly(cur, fund_db_id: int, period: str, holdings: list[dict]):
    for h in holdings:
        if not h.get("is_domestic"):
            continue
        cur.execute("""
            INSERT INTO tw.fund_holdings_quarterly
                (fund_id, period, ticker, ticker_name, weight)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (fund_id, period, ticker) DO UPDATE SET
                ticker_name = EXCLUDED.ticker_name,
                weight = EXCLUDED.weight
        """, (fund_db_id, period, h["ticker"], h["ticker_name"], h.get("weight")))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def scrape_monthly(period: str):
    """
    Scrape monthly Top 10 holdings from SITCA IN2629.

    Args:
        period: 'YYYYMM' format, e.g. '202603'
    """
    import time

    session = requests.Session()
    all_companies = list(FUND_REGISTRY.keys())

    with get_cursor() as cur:
        fund_id_map = _get_fund_id_map(cur)

        for company_code in all_companies:
            fund_list = FUND_REGISTRY[company_code]
            print(f"  Fetching SITCA monthly {period} for {company_code} ...")

            try:
                form_data = _get_form_fields(session, SITCA_URL)
                time.sleep(2)
                html = _post_query(session, SITCA_URL, form_data,
                                   company_code, period)
            except Exception as e:
                print(f"  [ERROR] SITCA request failed for {company_code}: {e}")
                continue

            parsed = _parse_monthly_holdings(html, fund_list)
            if not parsed:
                print(f"  [WARN] No matching funds found for {company_code}")
                continue

            for fund_code, holdings in parsed.items():
                fund_db_id = fund_id_map.get(fund_code)
                if not fund_db_id:
                    print(f"  [WARN] Fund {fund_code} not in DB, skipping")
                    continue
                _save_monthly(cur, fund_db_id, period, holdings)
                print(f"  {fund_code}: {len(holdings)} holdings saved")

            time.sleep(3)


def scrape_quarterly(period: str):
    """
    Scrape quarterly holdings from SITCA IN2630.

    Args:
        period: 'YYYYMM' format using quarter-end months (03/06/09/12),
                e.g. '202603' for Q1 2026
    """
    import time

    # Convert our period format to SITCA's format
    # IN2630 may use different period format — needs verification
    session = requests.Session()
    all_companies = list(FUND_REGISTRY.keys())

    with get_cursor() as cur:
        fund_id_map = _get_fund_id_map(cur)

        for company_code in all_companies:
            fund_list = FUND_REGISTRY[company_code]
            print(f"  Fetching SITCA quarterly {period} for {company_code} ...")

            try:
                form_data = _get_form_fields(session, SITCA_Q_URL)
                time.sleep(2)
                html = _post_query(session, SITCA_Q_URL, form_data,
                                   company_code, period)
            except Exception as e:
                print(f"  [ERROR] SITCA quarterly request failed for {company_code}: {e}")
                continue

            parsed = _parse_quarterly_holdings(html, fund_list)
            if not parsed:
                print(f"  [WARN] No matching funds for {company_code}")
                continue

            for fund_code, holdings in parsed.items():
                fund_db_id = fund_id_map.get(fund_code)
                if not fund_db_id:
                    print(f"  [WARN] Fund {fund_code} not in DB, skipping")
                    continue
                _save_quarterly(cur, fund_db_id, period, holdings)
                print(f"  {fund_code}: {len(holdings)} holdings saved")

            time.sleep(3)
