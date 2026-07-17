"""Inbound ports for the Vacinal domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IVacinacaoExtractionPort(Protocol):
    """Input port: extracts vaccination data from PNI API."""

    def extract(self, ano: str) -> dict: ...
