"""
Format shift detection utilities for historical data backfill.

ScrapeResult carries parse metrics so historical_update.py can detect
when an API response structure has changed over time.
"""

from dataclasses import dataclass
from datetime import date

# Shared by historical_update.py (backfill) and daily_update.py (daily run) so
# both flag a format shift on the same criteria.
SHIFT_ERROR_RATE = 0.40   # stop if >40% of API rows fail to parse
SHIFT_MIN_ROWS   = 10     # only check when API returned >= this many rows


class FormatShiftError(Exception):
    """Raised when a scraper detects that API response structure has changed."""

    def __init__(self, scraper: str, trade_date: date, details: str):
        self.scraper = scraper
        self.trade_date = trade_date
        self.details = details
        super().__init__(f"[{scraper}] Format shift on {trade_date}: {details}")


@dataclass
class ScrapeResult:
    """
    Structured return value from scrape_date() for monitoring and shift detection.

    records      : rows successfully saved to DB
    api_rows     : raw rows returned by API before parsing or security filtering
    parse_errors : rows that raised an exception during parsing
    failed_sources: the source legs (or per-item requests) that failed, each
                   entry naming the leg and, where known, why. Summing every
                   leg into `records` hides a half-dead run: one leg dying
                   still leaves records well above zero. For daily sources an
                   empty leg is a failure; for event-driven ones only a leg
                   that did not answer or could not be read is (see
                   failed_legs). Left empty by single-leg daily scrapers.
    """

    records: int
    api_rows: int
    parse_errors: int
    failed_sources: tuple[str, ...] = ()

    @property
    def error_rate(self) -> float:
        """Fraction of API rows that failed to parse (0.0–1.0)."""
        return self.parse_errors / self.api_rows if self.api_rows > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"records={self.records}, api_rows={self.api_rows}, "
            f"errors={self.parse_errors} ({self.error_rate:.0%})"
        )


# Outcome of one request to a source that can legitimately have nothing to
# report (an event board, a month with no filings). EMPTY is the source saying
# so in its own words; NO_RESPONSE and UNREADABLE are failures.
NO_RESPONSE = "no_response"   # request failed, timed out, or answered an HTTP error
UNREADABLE = "unreadable"     # answered, but not in the shape that carries data
EMPTY = "empty"               # answered: nothing to report
OK = "ok"                     # answered with rows

_FAILED_STATE_TEXT = {NO_RESPONSE: "沒有回應", UNREADABLE: "回應讀不懂"}


def twse_rwd_rows(data) -> tuple[list, str]:
    """Data rows of a TWSE /rwd/ JSON answer and the request's state. A quiet
    query is stat OK with an empty data list (probed 2026-10-17); any other
    stat means the query did not go through."""
    if data is None:
        return [], NO_RESPONSE
    if not isinstance(data, dict) or data.get("stat") != "OK" \
            or not isinstance(data.get("data"), list):
        return [], UNREADABLE
    return data["data"], OK if data["data"] else EMPTY


def openapi_rows(data) -> tuple[list, str]:
    """Rows of a TPEx OpenAPI answer, which is a bare JSON list ([] when
    there is nothing to report)."""
    if data is None:
        return [], NO_RESPONSE
    if not isinstance(data, list):
        return [], UNREADABLE
    return data, OK if data else EMPTY


def parsed_state(state: str, parsed: list) -> str:
    """Rows came back but none parsed: the layout changed under us."""
    return UNREADABLE if state == OK and not parsed else state


def failed_legs(legs: dict[str, str]) -> tuple[str, ...]:
    """failed_sources entries for the legs whose request did not succeed."""
    return tuple(f"{name}（{_FAILED_STATE_TEXT[state]}）"
                 for name, state in legs.items() if state in _FAILED_STATE_TEXT)
