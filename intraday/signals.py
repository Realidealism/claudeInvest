"""Signal engine skeleton (Phase B).

Reads the latest snapshot from tw.intraday_quotes every `interval_sec` seconds
and evaluates a small set of Python rules. Hits are printed for now; later they
can be persisted to a dedicated tw.intraday_alerts table, pushed to Slack, etc.

The current rule set is intentionally tiny — add your own signals below.
"""

import threading
import traceback
from dataclasses import dataclass
from typing import Callable

from db.connection import get_cursor


@dataclass
class Rule:
    name: str
    predicate: Callable[[dict], bool]
    action: Callable[[dict, "Rule"], None]


def _default_action(row: dict, rule: Rule):
    print(
        f"[SIGNAL] {rule.name}: {row['stock_id']} "
        f"last={row.get('last_price')} change%={row.get('change_pct')} "
        f"vol={row.get('total_volume')}"
    )


# Example rules — tune thresholds as needed.
DEFAULT_RULES: list[Rule] = [
    Rule(
        name="surge_up",
        predicate=lambda r: (r.get("change_pct") or 0) >= 5.0,
        action=_default_action,
    ),
    Rule(
        name="surge_down",
        predicate=lambda r: (r.get("change_pct") or 0) <= -5.0,
        action=_default_action,
    ),
    Rule(
        name="heavy_volume",
        predicate=lambda r: (r.get("total_value") or 0) >= 1_000_000_000,  # 10億台幣
        action=_default_action,
    ),
]


def _fetch_recent_snapshots(window_seconds: int = 120) -> list[dict]:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT stock_id, last_price, change_pct, total_volume, total_value,
                   amplitude, updated_at
            FROM tw.intraday_quotes
            WHERE updated_at > NOW() - make_interval(secs => %s)
            """,
            (window_seconds,),
        )
        return cur.fetchall()


def run(stop_event: threading.Event, interval_sec: int = 30,
        rules: list[Rule] | None = None):
    """Poll the latest snapshots and fire rules."""
    rules = rules or DEFAULT_RULES
    print(f"[SIGNAL] starting, interval={interval_sec}s, rules={len(rules)}")

    while not stop_event.is_set():
        try:
            rows = _fetch_recent_snapshots(window_seconds=interval_sec * 4)
            for row in rows:
                for rule in rules:
                    try:
                        if rule.predicate(row):
                            rule.action(row, rule)
                    except Exception:
                        print(f"[SIGNAL] [ERROR] rule {rule.name} failed on {row.get('stock_id')}:")
                        traceback.print_exc()
        except Exception:
            print("[SIGNAL] [ERROR] poll failed:")
            traceback.print_exc()

        if stop_event.wait(interval_sec):
            break

    print("[SIGNAL] stopping")
