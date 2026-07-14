"""PNI extractor — doses applied (vaccination).

Page/size pagination. Uses ExtractionService + PageNumberPagination (DIP + Strategy).
codigo_paciente arrives pre-hashed by the Ministry — no PII.
"""
from __future__ import annotations

import logging

from pipelines.extraction.adapters.pagination import PageNumberPagination
from pipelines.extraction.factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATH = "/vacinacao/doses-aplicadas-pni-2024"
_DATA_KEY = "doses_aplicadas_pni"
_REQUIRED_FIELDS = {"codigo_documento", "codigo_vacina", "data_vacina"}


def _validate(records: list[dict]) -> None:
    if records:
        missing = _REQUIRED_FIELDS - set(records[0].keys())
        if missing:
            raise ValueError(f"Required fields missing in vacinacao_pni: {missing}")


def run(ano: str | int = "2024") -> dict:
    cfg = make_pagination_config()
    paginator = PageNumberPagination(
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
        fonte="vacinacao_pni",
        data_ref=str(ano),
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {"row_count": result.row_count, "output_path": result.output_path, "source_url": result.source_url}
