"""
Shared helpers for TAIFEX (台灣期貨交易所) CSV download scrapers.

All TAIFEX endpoints used here are form POSTs that return Big5-encoded CSV
when the query is valid, and an HTML page (charset UTF-8) when the date range
is rejected or no data exists. download() returns the decoded CSV text, or
None for the HTML/error case so callers can distinguish "no data" from a
successful empty-day response.
"""

from __future__ import annotations

import csv
from datetime import date
from typing import Iterable

from utils.http_client import post


def download(url: str, data: dict, timeout: int = 120) -> str | None:
    """POST a TAIFEX download form; return decoded CSV text or None.

    None means either a network failure or the server returned its HTML
    fallback page (range too long / unsupported query). A valid response with
    no data rows still returns the header line as text.
    """
    resp = post(url, data=data, timeout=timeout)
    if resp is None:
        return None
    text = resp.content.decode("big5", errors="replace")
    if text.lstrip()[:1] == "<":
        return None
    return text


def rows(text: str) -> Iterable[list[str]]:
    """Yield data rows (header skipped) from CSV text."""
    reader = csv.reader(text.splitlines())
    next(reader, None)  # header
    for row in reader:
        if row:
            yield row


def num(s: str | None) -> float | None:
    """Parse a numeric cell. '-' / '' / '%' handled; returns None when blank."""
    s = (s or "").strip().replace(",", "").replace("%", "")
    if s in ("", "-", "－"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def integer(s: str | None) -> int | None:
    """Parse an integer cell (口數 / 金額 / 量). None when blank/'-'."""
    s = (s or "").strip().replace(",", "")
    if s in ("", "-", "－"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def date_slash(s: str | None) -> date | None:
    """Parse 'YYYY/MM/DD'."""
    s = (s or "").strip()
    try:
        y, m, d = s.split("/")
        return date(int(y), int(m), int(d))
    except (ValueError, AttributeError):
        return None


def date_compact(s: str | None) -> date | None:
    """Parse 'YYYYMMDD' (used by 契約到期日)."""
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def halted(s: str | None) -> bool:
    """是否因訊息面暫停交易: '*' / non-empty flag -> True."""
    return (s or "").strip() not in ("", "-")


def call_put(s: str | None) -> str | None:
    """買權/CALL -> 'C', 賣權/PUT -> 'P'."""
    s = (s or "").strip().upper()
    if s in ("買權", "CALL", "C"):
        return "C"
    if s in ("賣權", "PUT", "P"):
        return "P"
    return None
