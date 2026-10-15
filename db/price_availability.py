"""Which codes must not be served listed-market prices from tw.daily_prices.

Emerging-board (ESB) quotes were written into tw.daily_prices with the same
shape as listed prices.  Until they are removed, every production reader that
reads prices by code checks this module first and reports the code as
unavailable instead of reading prices or returning an empty result.

A code is unavailable (set U) when either holds:
  - tw.stocks marks it market = 'ESB', or
  - it has any row in the emerging-board shape (X): no open price and no
    turnover, but some close / high / low / volume.  Listed scrapers always
    write turnover, so listed rows never match X.

hermit_stock keeps a verbatim copy of UNAVAILABLE_CODES_TEMPLATE in its own
data/adapters/db_adapter.py; tests/test_price_availability_guards.py checks
the two texts are identical.
"""

UNAVAILABLE_MESSAGE = "不可用：興櫃證券，不提供上市櫃價格"

# {stocks} and {prices} are table expressions; production uses the real tables,
# tests substitute VALUES lists to exercise each branch of the definition.
UNAVAILABLE_CODES_TEMPLATE = """
    SELECT s.stock_id FROM {stocks} s WHERE s.market = 'ESB'
    UNION
    SELECT d.stock_id FROM {prices} d
    WHERE d.open_price IS NULL AND d.turnover IS NULL
      AND (d.close_price IS NOT NULL OR d.high_price IS NOT NULL
           OR d.low_price IS NOT NULL OR d.volume IS NOT NULL)
"""

# Subquery yielding every code in U; use as `stock_id NOT IN (...)`.
UNAVAILABLE_CODES_SQL = UNAVAILABLE_CODES_TEMPLATE.format(
    stocks="tw.stocks", prices="tw.daily_prices",
)


class PriceUnavailable(Exception):
    """The code is in U; its prices must not be read or reported."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(f"{code} {UNAVAILABLE_MESSAGE}")

    def __reduce__(self):
        # Rebuild from the code, not the message, when crossing a process boundary.
        return (type(self), (self.code,))


def unavailable_codes(cur, codes=None, *, sql: str = UNAVAILABLE_CODES_SQL) -> set[str]:
    """Return the codes in U.

    With ``codes`` given, only those of them that are in U; with None, all of U.
    ``sql`` exists for tests that run the same definition over VALUES lists.
    """
    if codes is None:
        cur.execute(sql)
    else:
        codes = list(codes)
        if not codes:
            return set()
        cur.execute(f"SELECT u.stock_id FROM ({sql}) u WHERE u.stock_id = ANY(%s)", (codes,))
    return {r["stock_id"] if isinstance(r, dict) else r[0] for r in cur.fetchall()}


def require_available(cur, code: str) -> None:
    """Raise PriceUnavailable when ``code`` is in U."""
    if unavailable_codes(cur, [code]):
        raise PriceUnavailable(code)
