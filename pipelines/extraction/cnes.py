"""Extrator CNES (Cadastro Nacional de Estabelecimentos de Saúde).

Paginação offset/limit. Usa ExtractionService + OffsetPagination (DIP + Strategy).
"""
from __future__ import annotations

import logging
from datetime import date

from pipelines.extraction.adapters.pagination import OffsetPagination
from pipelines.extraction.factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATH = "/cnes/estabelecimentos"
_DATA_KEY = "estabelecimentos"
_CAMPOS_MINIMOS = {"codigo_cnes", "nome_fantasia", "codigo_municipio"}


def _validate(records: list[dict]) -> None:
    if records:
        missing = _CAMPOS_MINIMOS - set(records[0].keys())
        if missing:
            raise ValueError(f"Campos obrigatórios ausentes no CNES: {missing}")


def run(snapshot_date: str | None = None) -> dict:
    snap = snapshot_date or date.today().strftime("%Y%m%d")
    cfg = make_pagination_config()
    paginator = OffsetPagination(
        base_url=cfg["base_url"],
        page_size=cfg["page_size"],
        max_retries=cfg["max_retries"],
    )
    source_url = f"{cfg['base_url']}{_PATH}"

    def fetch() -> list[dict]:
        records = paginator.fetch_all(_PATH, _DATA_KEY, {})
        _validate(records)
        return records

    service = make_extraction_service()
    result = service.run(
        fonte="cnes",
        data_ref=snap,
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {"row_count": result.row_count, "output_path": result.output_path, "source_url": result.source_url}
