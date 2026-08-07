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
    failed_sources: names of the source legs that came back empty, for scrapers
                   that pull from more than one endpoint (TWSE + TPEx, the four
                   odd-lot sessions, ...). Summing every leg into `records`
                   hides a half-dead run: one leg dying still leaves records
                   well above zero. Left empty by single-leg scrapers.
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
