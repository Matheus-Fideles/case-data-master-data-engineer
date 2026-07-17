"""Outbound ports for the Streaming domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IDeltaStoragePort(Protocol):
    """Output port: persists streaming micro-batches as Delta tables."""

    def merge(self, df, output_path: str, merge_key: str) -> None: ...


@runtime_checkable
class IDlqPort(Protocol):
    """Output port: sends invalid records to the Dead Letter Queue."""

    def send(self, df, reason: str) -> None: ...
