"""
Extract reference price + daily price limits from Shioaji contracts.

Shioaji's `api.Contracts.Stocks.TSE` and `.OTC` are populated at login time
with `Stock` objects exposing:

    code            e.g. "2330"
    name            e.g. "台積電"
    reference       today's reference price (參考價, usually = yesterday close)
    limit_up        reference × 1.10, rounded to tick
    limit_down      reference × 0.90, rounded to tick
    update_date     "YYYY/MM/DD"  (the day the metadata is valid for)
    exchange        Exchange.TSE / .OTC

We iterate both exchanges, normalise each row into a dict the store layer can
upsert, and drop rows that are missing the three load-bearing fields.
"""

from typing import TYPE_CHECKING, Iterable

from utils.classifier import classify_tw_security

if TYPE_CHECKING:
    # Avoid importing shioaji at module load: it silently injects a C-extension
    # module under the top-level name `utils`, shadowing our local utils/
    # package and breaking subsequent `from utils.X import ...` calls in any
    # module loaded afterwards. We only need Stock as a type hint.
    from shioaji.contracts import Stock


_EXCHANGE_TO_MARKET = {"TSE": "TWSE", "OTC": "TPEx"}


def fetch_reference_rows(api) -> list[dict]:
    """
    Walk api.Contracts.Stocks.TSE + .OTC and return one dict per stock with:
        stock_id, name, market, ref_price, limit_up, limit_down, update_date
    """
    rows: list[dict] = []
    rows.extend(_iter_exchange(api.Contracts.Stocks.TSE, "TSE"))
    rows.extend(_iter_exchange(api.Contracts.Stocks.OTC, "OTC"))
    return rows


def _iter_exchange(contract_namespace, exchange_name: str) -> Iterable[dict]:
    """Yield normalised dicts for every Stock under one exchange namespace."""
    market = _EXCHANGE_TO_MARKET[exchange_name]
    for contract in contract_namespace:
        row = _normalise_stock(contract, market)
        if row is not None:
            yield row


def _normalise_stock(contract: "Stock", market: str) -> dict | None:
    """
    Convert one Shioaji Stock contract into the dict shape store.upsert_reference
    expects. Returns None if the contract is missing fields we consider
    load-bearing (ref_price / limit_up / limit_down).
    """
    # Some delisted or suspended rows carry reference=0.0 — treat those as
    # missing so we don't poison the intraday snapshot with a bogus zero.
    ref = float(getattr(contract, "reference", 0) or 0)
    if ref <= 0:
        return None

    limit_up = float(getattr(contract, "limit_up", 0) or 0)
    limit_down = float(getattr(contract, "limit_down", 0) or 0)
    if limit_up <= 0 or limit_down <= 0:
        return None

    code = (getattr(contract, "code", "") or "").strip()
    if not code:
        return None

    # Shioaji returns the entire TSE+OTC universe: ~45K contracts covering
    # warrants, TDRs, ETNs, convertibles, etc. Filter down to the ones we
    # actually track in tw.stocks (stocks + ETFs) so we don't spam the DB
    # with 40K+ no-op INSERTs on each pre-market run.
    if classify_tw_security(code) is None:
        return None

    # day_trade enum: "Yes" / "No" → bool
    dt_raw = str(getattr(contract, "day_trade", "No") or "No")
    day_trade = dt_raw.lower() in ("yes", "daytrade.yes")

    return {
        "stock_id":    code,
        "name":        (getattr(contract, "name", "") or "").strip(),
        "market":      market,
        "ref_price":   ref,
        "limit_up":    limit_up,
        "limit_down":  limit_down,
        "update_date": getattr(contract, "update_date", "") or "",
        # Static extras — zero extra API cost, grabbed from the same contract
        "category":       (getattr(contract, "category", "") or "").strip() or None,
        "day_trade":      day_trade,
        "margin_balance": int(getattr(contract, "margin_trading_balance", 0) or 0) or None,
        "short_balance":  int(getattr(contract, "short_selling_balance", 0) or 0) or None,
    }
