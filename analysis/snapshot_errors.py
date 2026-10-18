"""Per-stock failures of the daily and intraday snapshots.

Both snapshots evaluate every active stock in a worker pool, and each step of
that evaluation (loading, the ScoreBoard, the six signal conditions, the two
unified factories and their backtests) used to swallow its exception. A
function broken for every stock then produced "no signal, no position today",
which looks exactly like a quiet day. The workers now record each swallowed
exception as (site, stock_id, "Type: message"); a run that collected any
raises PerStockErrors before it writes, so the previous snapshot stays in
place. Across 13 trading days (2026-07-01..10-16) no site ever fired.
"""
from __future__ import annotations

from collections import Counter, defaultdict

_SITES_SHOWN = 3
_STOCKS_SHOWN = 5


class PerStockErrors(RuntimeError):
    """A snapshot run in which some stock's evaluation raised."""

    def __init__(self, errors: list[tuple[str, str, str]]):
        self.errors = errors
        super().__init__(summarize(errors))

    def __reduce__(self):
        # args holds the summary, not the list; rebuild from the list.
        return type(self), (self.errors,)


def describe(e: BaseException) -> str:
    # One line: numpy/pandas messages often span several, and the nightly
    # summary keeps only the traceback's last three lines.
    return f"{type(e).__name__}: {' '.join(str(e).split())[:80]}"


_PRINTED = 50


def log_lines(errors: list[tuple[str, str, str]]) -> list[str]:
    """The log's view: the first few failures one per line, then a count. A
    step broken for every stock would otherwise print a line per stock on
    every intraday pass."""
    lines = [f"  [ERROR] {sid} {site}: {msg}" for site, sid, msg in errors[:_PRINTED]]
    if len(errors) > _PRINTED:
        lines.append(f"  [ERROR] ... and {len(errors) - _PRINTED} more")
    return lines


def summarize(errors: list[tuple[str, str, str]]) -> str:
    """One line: how many failures, on how many stocks, grouped by site
    (most frequent first, at most three), each with its commonest message,
    then a few stock ids."""
    by_site: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for site, sid, msg in errors:
        by_site[site].append((sid, msg))
    stocks = sorted({sid for _, sid, _ in errors})
    parts = []
    for site, hits in sorted(by_site.items(), key=lambda kv: -len(kv[1]))[:_SITES_SHOWN]:
        msg = Counter(m for _, m in hits).most_common(1)[0][0]
        parts.append(f"{site} {len({s for s, _ in hits})} 檔（{msg}）")
    more = len(by_site) - _SITES_SHOWN
    return (f"逐檔例外 {len(errors)} 處（{len(stocks)} 檔）：{'；'.join(parts)}"
            f"{f'；另 {more} 個位置' if more > 0 else ''}"
            f"。例：{'、'.join(stocks[:_STOCKS_SHOWN])}")
