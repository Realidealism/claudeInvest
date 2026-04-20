"""Strategy registry — auto-discovers all BaseStrategy subclasses."""

import importlib
import json
import pkgutil
from datetime import date
from pathlib import Path

from strategies.base import BaseStrategy

_registry: list[type[BaseStrategy]] = []


def _discover():
    """Import all modules in this package to trigger class registration."""
    pkg_dir = Path(__file__).parent
    for info in pkgutil.iter_modules([str(pkg_dir)]):
        if info.name in ("base", "registry", "__init__"):
            continue
        importlib.import_module(f"strategies.{info.name}")


def register(cls: type[BaseStrategy]):
    """Decorator to register a strategy class."""
    _registry.append(cls)
    return cls


def get_strategies() -> list[BaseStrategy]:
    """Return instances of all registered strategies."""
    if not _registry:
        _discover()
    return [cls() for cls in _registry]


def scan_all(period: str, cur) -> list[dict]:
    """Run all registered strategies and collect signals."""
    signals = []
    for strategy in get_strategies():
        signals.extend(strategy.scan(period, cur))
    return signals


def _json_serial(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def save_signals(signals: list[dict], cur) -> int:
    """Upsert signals into tw.signals. Returns number of rows upserted."""
    n = 0
    for s in signals:
        evidence_json = json.dumps(s["evidence"], default=_json_serial,
                                   ensure_ascii=False)
        cur.execute("""
            INSERT INTO tw.signals
                (signal_type, ticker, ticker_name, funds,
                 trigger_date, trigger_period, weight_change, evidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (signal_type, ticker, trigger_period) DO UPDATE SET
                ticker_name = EXCLUDED.ticker_name,
                funds = EXCLUDED.funds,
                trigger_date = EXCLUDED.trigger_date,
                weight_change = EXCLUDED.weight_change,
                evidence = EXCLUDED.evidence
        """, (s["signal_type"], s["ticker"], s["ticker_name"],
              s["funds"], s["trigger_date"], s["trigger_period"],
              s["weight_change"], evidence_json))
        n += 1
    return n
