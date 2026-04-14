"""
Format shift detection utilities for historical data backfill.

ScrapeResult carries parse metrics so historical_update.py can detect
when an API response structure has changed over time.
"""

from dataclasses import dataclass
from datetime import date


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
    """

    records: int
    api_rows: int
    parse_errors: int

    @property
    def error_rate(self) -> float:
        """Fraction of API rows that failed to parse (0.0–1.0)."""
        return self.parse_errors / self.api_rows if self.api_rows > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"records={self.records}, api_rows={self.api_rows}, "
            f"errors={self.parse_errors} ({self.error_rate:.0%})"
        )
