"""Base class for fund-manager behavior signals."""

from abc import ABC, abstractmethod
from datetime import date


class BaseStrategy(ABC):
    """Abstract base for all strategy signal detectors."""

    @property
    @abstractmethod
    def signal_type(self) -> str:
        """Unique identifier for this signal type."""

    @abstractmethod
    def scan(self, period: str, cur, trade_date: date | None = None) -> list[dict]:
        """Scan for signals in the given period.

        Args:
            period: 'YYYYMM' for monthly, 'YYYYMM' (quarter-end month) for quarterly.
            cur: DB cursor (psycopg2 DictCursor).
            trade_date: The actual trading date to use as trigger_date.

        Returns:
            List of signal dicts with keys:
                signal_type, ticker, ticker_name, funds, trigger_date,
                trigger_period, weight_change, evidence
        """

    def scan_daily(self, trade_date, cur) -> list[dict]:
        """Optional: scan using a specific trade date instead of monthly period.

        Subclasses that operate on daily/weekly granularity should override this.
        By default, falls back to monthly scan.
        """
        period = trade_date.strftime("%Y%m")
        return self.scan(period, cur, trade_date)

    def _make_signal(self, *, ticker: str, ticker_name: str,
                     funds: list[str], trigger_period: str,
                     trigger_date: date | None = None,
                     weight_change: float | None = None,
                     evidence: dict | None = None) -> dict:
        return {
            "signal_type": self.signal_type,
            "ticker": ticker,
            "ticker_name": ticker_name,
            "funds": funds,
            "trigger_date": trigger_date or date.today(),
            "trigger_period": trigger_period,
            "weight_change": weight_change,
            "evidence": evidence or {},
        }
