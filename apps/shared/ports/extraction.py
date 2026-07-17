"""Ports (interfaces) for the extraction layer — Hexagonal Architecture.

Domain logic (WHAT to extract and WHERE to store) is expressed in terms
of these protocols. Concrete adapters are in adapters/.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class DataSourcePort(Protocol):
    """Input port: anything that provides raw records."""

    def fetch(self) -> list[dict]:
        """Returns all available records."""
        ...


@runtime_checkable
class LandingStoragePort(Protocol):
    """Output port: anything that persists records in the landing zone."""

    def write(
        self,
        records: list[dict],
        fonte: str,
        data_ref: str,
        source_url: str,
    ) -> str:
        """Persists records and returns the output path (s3a:// or file://)."""
        ...


@runtime_checkable
class CachePort(Protocol):
    """Secondary output port: local cache for OFFLINE_MODE."""

    def save(self, records: list[dict], fonte: str, data_ref: str) -> None: ...

    def load(self, fonte: str, data_ref: str) -> list[dict]: ...
