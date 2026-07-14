"""Strategy Pattern: estratégias de paginação para APIs REST.

Resolve o problema de duas funções quase idênticas (fetch_paginated e
fetch_paginated_by_page) — cada estratégia encapsula um contrato de paginação
diferente sem duplicar código.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)


def _make_retry(max_attempts: int):
    return retry(
        retry=retry_if_exception_type(
            (requests.HTTPError, requests.ConnectionError, requests.Timeout)
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(max_attempts),
        reraise=True,
    )


class PaginationStrategy(ABC):
    """Porta interna: contrato de paginação."""

    def __init__(self, base_url: str, page_size: int, max_retries: int) -> None:
        self._base_url = base_url
        self._page_size = page_size
        self._max_retries = max_retries

    def fetch_all(self, path: str, data_key: str, params: dict) -> list[dict]:
        """Template Method: loop de paginação com estratégia plugável."""
        results: list[dict] = []
        state = self._initial_state()
        while True:
            page_params = dict(params)
            self._apply_state(page_params, state)
            page = self._fetch_page(f"{self._base_url}{path}", page_params, data_key)
            results.extend(page)
            log.info("GET %s%s → %d registros (total: %d)", self._base_url, path, len(page), len(results))
            if len(page) < self._page_size:
                break
            state = self._next_state(state, len(page))
        return results

    def _fetch_page(self, url: str, params: dict, data_key: str) -> list[dict]:
        resp = self._get(url, params)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", 10)))
            resp.raise_for_status()
        resp.raise_for_status()
        items = resp.json().get(data_key, [])
        return items if isinstance(items, list) else []

    def _get(self, url: str, params: dict) -> requests.Response:
        decorated = _make_retry(self._max_retries)(requests.get)
        return decorated(url, params=params, timeout=30)

    @abstractmethod
    def _initial_state(self) -> dict: ...

    @abstractmethod
    def _apply_state(self, params: dict, state: dict) -> None: ...

    @abstractmethod
    def _next_state(self, state: dict, page_len: int) -> dict: ...


class OffsetPagination(PaginationStrategy):
    """Estratégia offset/limit — usada por arboviroses, CNES e SIM."""

    def __init__(
        self,
        base_url: str,
        page_size: int,
        max_retries: int,
        offset_param: str = "offset",
    ) -> None:
        super().__init__(base_url, page_size, max_retries)
        self._offset_param = offset_param

    def _initial_state(self) -> dict:
        return {"offset": 0, "limit": self._page_size}

    def _apply_state(self, params: dict, state: dict) -> None:
        params[self._offset_param] = state["offset"]
        params["limit"] = state["limit"]

    def _next_state(self, state: dict, page_len: int) -> dict:
        return {"offset": state["offset"] + page_len, "limit": state["limit"]}


class PageNumberPagination(PaginationStrategy):
    """Estratégia page/size — usada pela API de vacinação PNI."""

    def _initial_state(self) -> dict:
        return {"page": 0, "size": self._page_size}

    def _apply_state(self, params: dict, state: dict) -> None:
        params["page"] = state["page"]
        params["size"] = state["size"]

    def _next_state(self, state: dict, _page_len: int) -> dict:
        return {"page": state["page"] + 1, "size": state["size"]}
