"""Municipality extractor (IBGE × health regions).

API returns all ~5570 municipalities in a single call — uses SingleCallStrategy.
No pagination. Uses ExtractionService (DIP) + SingleCallStrategy.
"""
from __future__ import annotations

import logging
from datetime import date

from pipelines.extraction.adapters.single_call import SingleCallStrategy
from pipelines.extraction.factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATH = "/macrorregiao-e-regiao-de-saude/municipio"
_DATA_KEY = "macrorregiao_regiao_saude_municipios"
_REQUIRED_FIELDS = {"codigo_municipio", "municipio", "uf"}


def _validate(records: list[dict]) -> None:
    if not records:
        raise ValueError("API returned empty municipality list")
    missing = _REQUIRED_FIELDS - set(records[0].keys())
    if missing:
        raise ValueError(f"Required fields missing in municipalities: {missing}")


def run(snapshot_date: str | None = None) -> dict:
    snap = snapshot_date or date.today().strftime("%Y%m%d")
    cfg = make_pagination_config()
    fetcher = SingleCallStrategy(
        base_url=cfg["base_url"],
        max_retries=cfg["max_retries"],
    )
    source_url = f"{cfg['base_url']}{_PATH}"

    def fetch() -> list[dict]:
        records = fetcher.fetch_all(_PATH, _DATA_KEY, {})
        _validate(records)
        return records

    service = make_extraction_service()
    result = service.run(
        fonte="municipios",
        data_ref=snap,
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {"row_count": result.row_count, "output_path": result.output_path, "source_url": result.source_url}
