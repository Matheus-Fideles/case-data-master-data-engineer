"""Inbound ports for the Hospitalar domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ISimObitosExtractionPort(Protocol):
    """Input port: extracts mortality data from SIM API."""

    def extract(self, ano: str) -> dict: ...


@runtime_checkable
class ICnesExtractionPort(Protocol):
    """Input port: extracts establishment data from CNES API."""

    def extract(self, snapshot_date: str) -> dict: ...
