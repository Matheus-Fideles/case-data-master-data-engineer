"""Adapter: CachePort → sistema de arquivos local.

Usado quando OFFLINE_MODE=True para evitar chamadas às APIs externas.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from pipelines.extraction.ports import CachePort

log = logging.getLogger(__name__)


class LocalCacheAdapter:
    """Implementa CachePort usando o diretório data/raw/."""

    def __init__(self, root: Path | str = "data/raw") -> None:
        self._root = Path(root)

    def save(self, records: list[dict], fonte: str, data_ref: str) -> None:
        cache_dir = self._root / fonte / data_ref
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / "data.json"
        out.write_text(json.dumps(records, ensure_ascii=False, default=str))
        log.info("Cache: %d registros salvos em %s", len(records), out)

    def load(self, fonte: str, data_ref: str) -> list[dict]:
        cache_dir = self._root / fonte / data_ref
        records: list[dict] = []
        for f in sorted(cache_dir.glob("*.json")):
            data = json.loads(f.read_text())
            records.extend(data if isinstance(data, list) else [data])
        log.info("Cache: %d registros carregados de %s", len(records), cache_dir)
        return records
