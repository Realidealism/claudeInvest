"""SinoPac Shioaji snapshot fallback for the REST sweeper.

When the primary data source (E.Sun REST) fails consecutively, the sweeper
falls back to api.snapshots() on the already-logged-in Shioaji instance.

The main entry point is fetch_snapshot_quotes(api, contracts) which returns
the same list[dict] shape as esun_rest.fetch_snapshot_quotes so the store
layer can upsert them identically.

Import-order caveat: this module imports shioaji types at runtime. Any caller
must ensure `utils.*` is imported BEFORE this module if they need both.
See sinopac_reference.py's TYPE_CHECKING trick for background.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import shioaji as sj


def fetch_snapshot_quotes(api: "sj.Shioaji", contracts: list) -> list[dict]:
    """Fetch snapshots for pre-filtered contracts and normalize to DB shape.

    contracts: list of Stock contract objects (already filtered to stocks+ETFs
               by the caller at startup time).
    Returns the same dict shape as esun_rest._normalize_snapshot.
    """
    if not contracts:
        return []

    raw = api.snapshots(contracts)
    out: list[dict] = []
    for snap in raw:
        row = _normalize_snapshot(snap)
        if row is not None:
            out.append(row)
    return out


def _normalize_snapshot(snap) -> dict | None:
    """Convert one Shioaji Snapshot object to the tw.intraday_quotes shape."""
    code = getattr(snap, "code", "")
    if not code:
        return None

    close = float(getattr(snap, "close", 0) or 0)
    if close <= 0:
        return None

    # Shioaji total_volume is in lots (張); convert to shares (股)
    total_vol_lots = int(getattr(snap, "total_volume", 0) or 0)

    return {
        "stock_id":      code,
        "name":          None,   # Shioaji snapshot doesn't carry name
        "open_price":    float(getattr(snap, "open", 0) or 0) or None,
        "high_price":    float(getattr(snap, "high", 0) or 0) or None,
        "low_price":     float(getattr(snap, "low", 0) or 0) or None,
        "last_price":    close,
        "last_size":     int(getattr(snap, "volume", 0) or 0) or None,
        "last_trade_at": None,   # Shioaji snapshot has ts but in nanoseconds epoch
        "total_volume":  total_vol_lots * 1000 if total_vol_lots else None,
        "total_value":   int(getattr(snap, "total_amount", 0) or 0) or None,
        "tx_count":      None,   # Not available in Shioaji snapshot
        "change_price":  float(getattr(snap, "change_price", 0) or 0) or None,
        "change_pct":    float(getattr(snap, "change_rate", 0) or 0) or None,
        "amplitude":     None,   # Not directly available
        "limit_up":      None,   # Already set by pre-market path; don't overwrite
        "limit_down":    None,
    }
