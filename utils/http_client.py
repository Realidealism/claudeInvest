import time
import random
import requests
from functools import wraps

# Shared session for connection reuse
_session = None

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Track last request time per domain to enforce minimum interval
_last_request_time: dict[str, float] = {}

# Minimum interval between requests to the same domain (seconds)
MIN_INTERVAL = 5.0
# Random jitter added on top of minimum interval (seconds)
MAX_JITTER = 2.0
# Max retries on failure
MAX_RETRIES = 3
# Base wait time for exponential backoff (seconds)
BACKOFF_BASE = 5.0


def _get_domain(url: str) -> str:
    """Extract domain from URL for per-domain rate limiting."""
    from urllib.parse import urlparse
    return urlparse(url).netloc


def _wait_for_rate_limit(domain: str):
    """Enforce minimum interval between requests to the same domain."""
    now = time.time()
    last = _last_request_time.get(domain, 0)
    elapsed = now - last

    required = MIN_INTERVAL + random.uniform(0, MAX_JITTER)
    if elapsed < required:
        wait = required - elapsed
        time.sleep(wait)

    _last_request_time[domain] = time.time()


def get_session() -> requests.Session:
    """Get or create a shared requests session."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(DEFAULT_HEADERS)
    return _session


def fetch(url: str, params: dict = None, timeout: int = 30) -> requests.Response | None:
    """
    Fetch URL with rate limiting, retry, and exponential backoff.
    Returns Response on success, None on failure.
    """
    domain = _get_domain(url)
    session = get_session()

    for attempt in range(MAX_RETRIES):
        try:
            _wait_for_rate_limit(domain)
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None
            print(f"  HTTP {status} on attempt {attempt + 1}: {e}")

            # 429 Too Many Requests or 503: back off aggressively
            if status in (429, 503):
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(1, 3)
                print(f"  Rate limited. Waiting {wait:.1f}s ...")
                time.sleep(wait)
            elif attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE)
            else:
                return None

        except requests.exceptions.ConnectionError as e:
            print(f"  Connection error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                wait = BACKOFF_BASE * (2 ** attempt)
                time.sleep(wait)
            else:
                return None

        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE)
            else:
                return None

    return None


def fetch_json(url: str, params: dict = None, timeout: int = 30) -> dict | None:
    """Fetch URL and parse JSON. Returns dict on success, None on failure."""
    resp = fetch(url, params=params, timeout=timeout)
    if resp is None:
        return None
    try:
        return resp.json()
    except ValueError as e:
        print(f"  JSON parse error: {e}")
        return None


# Soft-failure retry settings
SOFT_RETRY_COUNT = 2
SOFT_RETRY_WAIT  = 10  # seconds


def fetch_json_retry(
    url: str,
    params: dict = None,
    timeout: int = 30,
    retries: int = SOFT_RETRY_COUNT,
    wait: float = SOFT_RETRY_WAIT,
    validate=None,
) -> dict | None:
    """
    Fetch JSON with retry on soft failures (HTTP 200 but invalid data).

    validate(data) -> bool: returns True if the response is considered valid.
    When validate is given and the fetched data fails validation, the call is
    retried up to `retries` times (total attempts = retries + 1).
    """
    data = None
    for attempt in range(retries + 1):
        data = fetch_json(url, params=params, timeout=timeout)
        if data is not None and (validate is None or validate(data)):
            return data
        if attempt < retries:
            print(f"  [RETRY] Soft failure (attempt {attempt + 1}/{retries + 1}), "
                  f"retrying in {wait}s ...")
            time.sleep(wait)
    return data