"""Application Service: orchestrates extraction without depending on infrastructure.

Receives ports via dependency injection (DIP).
Does not import boto3, requests, or any SDK directly.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from pipelines.extraction.ports import CachePort, LandingStoragePort

log = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    row_count: int
    output_path: str
    source_url: str


class ExtractionService:
    """Orchestrates fetch → (cache) → landing with DIP on both ports."""

    def __init__(
        self,
        landing: LandingStoragePort,
        cache: CachePort,
        offline: bool = False,
    ) -> None:
        self._landing = landing
        self._cache = cache
        self._offline = offline

    def run(
        self,
        fonte: str,
        data_ref: str,
        fetch_fn: Callable[[], list[dict]],
        source_url: str,
    ) -> ExtractionResult:
        if self._offline:
            records = self._cache.load(fonte, data_ref)
        else:
            records = fetch_fn()
            self._cache.save(records, fonte, data_ref)

        if not records:
            raise ValueError(f"No records obtained for {fonte}/{data_ref}")

        output_path = self._landing.write(records, fonte, data_ref, source_url)
        log.info("Extraction complete: %d records → %s", len(records), output_path)
        return ExtractionResult(
            row_count=len(records),
            output_path=output_path,
            source_url=source_url,
        )
