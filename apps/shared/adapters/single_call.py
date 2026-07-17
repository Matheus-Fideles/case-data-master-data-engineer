"""Adapter: Strategy for APIs without pagination (single call).

Implements the same interface as PaginationStrategy but makes a single
HTTP request — used by the municipality API which returns ~5570 records.
"""

from __future__ import annotations

import logging
import time

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)


class SingleCallStrategy:
    """Fetches all records in a single GET request."""

    def __init__(self, base_url: str, max_retries: int = 5, timeout: int = 60) -> None:
        self._base_url = base_url
        self._max_retries = max_retries
        self._timeout = timeout

    def fetch_all(self, path: str, data_key: str, params: dict) -> list[dict]:
        url = f"{self._base_url}{path}"
        log.info("GET %s (single call)", url)

        @retry(
            retry=retry_if_exception_type(
                (requests.HTTPError, requests.ConnectionError, requests.Timeout)
            ),
            wait=wait_exponential(multiplier=2, min=2, max=60),
            stop=stop_after_attempt(self._max_retries),
            reraise=True,
        )
        def _get():
            resp = requests.get(url, params=params, timeout=self._timeout)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 10)))
                resp.raise_for_status()
            resp.raise_for_status()
            return resp

        items = _get().json().get(data_key, [])
        records = items if isinstance(items, list) else []
        log.info("Fetched %d records from %s", len(records), url)
        return records
