"""Inbound ports for the Epidemiologico domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IArboviroseExtractionPort(Protocol):
    """Input port: extracts arbovirus data from SINAN API."""

    def extract(self, agravo: str, ano: int) -> dict: ...


@runtime_checkable
class IBronzeIngestionPort(Protocol):
    """Input port: ingests raw JSON into Delta Lake Bronze."""

    def ingest(
        self, input_path: str, output_path: str, partition_val: str, batch_id: str
    ) -> int: ...
