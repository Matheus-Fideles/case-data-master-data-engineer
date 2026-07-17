"""SIM extractor — Mortality Information System.

Offset/limit pagination with year parameter. Uses ExtractionService + OffsetPagination.
"""

from __future__ import annotations

import logging
import os

from apps.shared.adapters.pagination import OffsetPagination
from apps.shared.extraction_factory import make_extraction_service, make_pagination_config

log = logging.getLogger(__name__)

_PATH = "/vigilancia-e-meio-ambiente/sistema-de-informacao-sobre-mortalidade"
_DATA_KEY = "sim"
_REQUIRED_FIELDS = {"contador", "causabas", "dtobito"}


def _validate(records: list[dict]) -> None:
    if records:
        missing = _REQUIRED_FIELDS - set(records[0].keys())
        if missing:
            raise ValueError(f"Required fields missing in SIM: {missing}")


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
        fonte="hospitalar/sim_obitos",
        data_ref=ano,
        fetch_fn=fetch,
        source_url=source_url,
    )
    return {
        "row_count": result.row_count,
        "output_path": result.output_path,
        "source_url": result.source_url,
    }
