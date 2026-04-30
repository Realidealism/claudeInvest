"""
Shioaji (SinoPac) API loader for market-data-only usage.

We never place orders, so no CA certificate is needed — a plain api_key +
secret_key login is enough. `fetch_contract=True` (the default) is what
populates `api.Contracts.Stocks.TSE` / `.OTC` with the contract metadata
(reference price, limit up/down, etc.) that pre_market_update.py reads.

Import-order caveat: shioaji transitively loads a C-extension under the
top-level name `utils`, which shadows our own utils/ package. Any module
that imports `shioaji.*` at the top level AND also does
`from utils.X import ...` later on the same file will crash. The fix is
to keep `shioaji.*` imports out of module-level code in those files —
see intraday/sinopac_reference.py's TYPE_CHECKING trick.
"""

import shioaji as sj

from config.settings import SINOPAC_API_KEY, SINOPAC_SECRET_KEY


def load_api() -> sj.Shioaji:
    """
    Log in to Shioaji and return an api instance with contracts already
    fetched. Fails fast if credentials are missing.
    """
    if not SINOPAC_API_KEY or not SINOPAC_SECRET_KEY:
        raise RuntimeError(
            "SINOPAC_API_KEY / SINOPAC_SECRET_KEY not set in .env — "
            "cannot log in to Shioaji."
        )

    api = sj.Shioaji(simulation=True)
    print("[SINOPAC] logging in ...")
    api.login(
        api_key=SINOPAC_API_KEY,
        secret_key=SINOPAC_SECRET_KEY,
        fetch_contract=True,
    )
    print("[SINOPAC] login ok")
    return api


def logout_api(api: sj.Shioaji) -> None:
    """Best-effort logout. Swallows errors so it's safe in finally blocks."""
    try:
        api.logout()
    except Exception as e:
        print(f"[SINOPAC] logout failed (ignored): {e}")
