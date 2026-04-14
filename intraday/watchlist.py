"""Load the Taiwan watchlist for the WebSocket fine-tracking layer.

The watchlist lives in portfolio.watchlist (db/migrations/003_init_portfolio.sql).
That table is shared across markets so we filter WHERE market='TW'.
"""

from db.connection import get_cursor
from utils.classifier import classify_tw_security


def load_tw_watchlist(include_etf: bool = True) -> list[str]:
    """Return the list of TW symbols the watcher should subscribe to.

    Skips anything classify_tw_security can't recognize (warrants, TDR, bonds…).
    include_etf=True keeps STOCK + EQUITY_ETF + BOND_ETF; False keeps only STOCK.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(
            "SELECT symbol FROM portfolio.watchlist WHERE market = 'TW' ORDER BY symbol"
        )
        rows = cur.fetchall()

    symbols: list[str] = []
    for row in rows:
        sid = row["symbol"].strip()
        sec_type = classify_tw_security(sid)
        if sec_type is None:
            continue
        if not include_etf and sec_type != "STOCK":
            continue
        symbols.append(sid)
    return symbols
