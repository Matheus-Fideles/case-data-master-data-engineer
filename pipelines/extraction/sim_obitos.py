"""Extrator SIM — Sistema de Informação sobre Mortalidade.

Paginação offset/limit com parâmetro ano. Usa ExtractionService + OffsetPagination.
"""
from __future__ import annotations

import logging
import os

from pipelines.extraction.adapters.pagination import OffsetPagination
from pipelines.extraction.factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATH = "/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade"
_DATA_KEY = "sim"
_CAMPOS_MINIMOS = {"contador", "causabas", "dtobito"}


def _validate(records: list[dict]) -> None:
    if records:
        missing = _CAMPOS_MINIMOS - set(records[0].keys())
        if missing:
            raise ValueError(f"Campos obrigatórios ausentes no SIM: {missing}")


def run(ano: str | int | None = None) -> dict:
    ano = str(ano or os.environ.get("DATA_REF_ANO", "2024"))
    cfg = make_pagination_config()
    paginator = OffsetPagination(
        base_url=cfg["base_url"],
        page_size=cfg["page_size"],
        max_retries=cfg["max_retries"],
    )
    source_url = f"{cfg['base_url']}{_PATH}?ano={ano}"

    def fetch() -> list[dict]:
        records = paginator.fetch_all(_PATH, _DATA_KEY, {"ano": ano})
        _validate(records)
        return records

    service = make_extraction_service()
    result = service.run(
        fonte="sim",
        data_ref=ano,
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {"row_count": result.row_count, "output_path": result.output_path, "source_url": result.source_url}
