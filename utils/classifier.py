import re


def classify_tw_security(stock_id: str) -> str | None:
    """
    Classify a Taiwan security by its code.
    Returns: 'STOCK', 'EQUITY_ETF', 'BOND_ETF', or None (skip).
    """
    sid = stock_id.strip()

    # Bond ETF: ends with B (e.g. 00679B, 00687B)
    if re.match(r"^00\d{3}B$", sid):
        return "BOND_ETF"

    # Equity ETF: 00xxx, 00xxxL, 00xxxR, 00xxxU (futures/commodity),
    #             00xxxC (bond+call variant), 006xxx, 0050-style 4-digit
    if re.match(r"^00\d{2,3}[LRUC]?$", sid):
        return "EQUITY_ETF"
    if re.match(r"^006\d{3}$", sid):
        return "EQUITY_ETF"

    # Common stock: 4-digit starting with 1-9
    if re.match(r"^[1-9]\d{3}$", sid):
        return "STOCK"

    # Everything else (warrants, ETN, TDR, etc.): skip
    return None