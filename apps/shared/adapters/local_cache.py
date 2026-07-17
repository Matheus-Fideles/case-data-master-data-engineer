"""Adapter: CachePort → local filesystem.

Used when OFFLINE_MODE=True to avoid calls to external APIs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class LocalCacheAdapter:
    """Implements CachePort using the data/raw/ directory."""

    def __init__(self, root: Path | str = "data/raw") -> None:
        self._root = Path(root)

    def save(self, records: list[dict], fonte: str, data_ref: str) -> None:
        cache_dir = self._root / fonte / data_ref
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / "data.json"
        out.write_text(json.dumps(records, ensure_ascii=False, default=str))
        log.info("Cache: %d records saved to %s", len(records), out)

    def load(self, fonte: str, data_ref: str) -> list[dict]:
        cache_dir = self._root / fonte / data_ref
        records: list[dict] = []
        for f in sorted(cache_dir.glob("*.json")):
            data = json.loads(f.read_text())
            records.extend(data if isinstance(data, list) else [data])
        log.info("Cache: %d records loaded from %s", len(records), cache_dir)
        return records
