"""Inbound ports for the OLTP domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IOltpSnapshotPort(Protocol):
    """Input port: extracts OLTP patient snapshot from Postgres."""

    def extract(self, snapshot_date: str, incremental: bool) -> dict: ...
