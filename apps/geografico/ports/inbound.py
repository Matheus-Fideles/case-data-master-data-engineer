"""Inbound ports for the Geografico domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IMunicipiosExtractionPort(Protocol):
    """Input port: extracts municipality data from IBGE API."""

    def extract(self, snapshot_date: str) -> dict: ...
