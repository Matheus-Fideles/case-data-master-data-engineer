"""Outbound ports for the OLTP domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ILandingStoragePort(Protocol):
    """Output port: persists raw OLTP records to landing zone."""

    def write(self, records: list[dict], fonte: str, data_ref: str, source_url: str) -> str: ...
