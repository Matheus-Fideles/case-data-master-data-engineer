"""Outbound ports for the Hospitalar domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IDeltaStoragePort(Protocol):
    """Output port: persists DataFrames as Delta tables on MinIO."""

    def write(self, df, output_path: str, partition_col: str, partition_val: str) -> int: ...


@runtime_checkable
class ILineagePort(Protocol):
    """Output port: emits data lineage events to Marquez."""

    def emit_job_start(self, job_name: str, inputs: list, outputs: list) -> str | None: ...

    def emit_job_complete(self, job_name: str, run_id: str, row_count: int) -> None: ...
