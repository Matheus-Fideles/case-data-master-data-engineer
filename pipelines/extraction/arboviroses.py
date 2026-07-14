"""Arbovirus extractor (dengue, zika, chikungunya) — SINAN API.

Pagination pattern: offset/limit with data_key "parametros".
Uses ExtractionService (DIP) + OffsetPagination (Strategy Pattern).
"""

from __future__ import annotations

import logging

from pipelines.extraction.adapters.pagination import OffsetPagination
from pipelines.extraction.factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATHS: dict[str, str] = {
    "dengue": "/arboviroses/dengue",
    "zika": "/arboviroses/zikavirus",
    "chikungunya": "/arboviroses/chikungunya",
}

_REQUIRED_FIELDS = {"id_agravo", "dt_notific", "id_municip"}


def _validate(records: list[dict], agravo: str) -> None:
    if not records:
        return
    missing = _REQUIRED_FIELDS - set(records[0].keys())
    if missing:
        raise ValueError(f"Required fields missing in {agravo}: {missing}")


def run(agravo: str, ano: int | str = 2024) -> dict:
    """Entrypoint called by PythonOperator in Airflow."""
    path = _PATHS[agravo]  # intentional KeyError for invalid agravo
    cfg = make_pagination_config()
    paginator = OffsetPagination(
        base_url=cfg["base_url"],
        page_size=cfg["page_size"],
        max_retries=cfg["max_retries"],
    )
    source_url = f"{cfg['base_url']}{path}?nu_ano={ano}"

    def fetch() -> list[dict]:
        records = paginator.fetch_all(path, "parametros", {"nu_ano": str(ano)})
        _validate(records, agravo)
        return records

    service = make_extraction_service()
    result = service.run(
        fonte=agravo,
        data_ref=str(ano),
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {
        "row_count": result.row_count,
        "output_path": result.output_path,
        "source_url": result.source_url,
    }
