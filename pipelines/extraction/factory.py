"""Factory: compõe os adapters com o serviço de aplicação.

Único lugar que lê variáveis de ambiente e instancia infraestrutura.
Os extratores individuais chamam `make_extraction_service()` e recebem
um ExtractionService pronto — sem acoplamento direto ao boto3 ou ao filesystem.
"""
from __future__ import annotations

import os

from pipelines.extraction.adapters.local_cache import LocalCacheAdapter
from pipelines.extraction.adapters.s3_landing import S3LandingAdapter
from pipelines.extraction.service import ExtractionService


def make_extraction_service() -> ExtractionService:
    """Compõe e retorna um ExtractionService configurado pelo ambiente."""
    landing = S3LandingAdapter(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        bucket=os.environ.get("MINIO_BUCKET_LANDING", "landing"),
    )
    cache = LocalCacheAdapter(root=os.environ.get("DATA_RAW_DIR", "data/raw"))
    offline = os.environ.get("OFFLINE_MODE", "0") == "1"

    return ExtractionService(landing=landing, cache=cache, offline=offline)


def make_pagination_config() -> dict:
    """Retorna configurações de paginação lidas do ambiente."""
    return {
        "base_url": os.environ.get("API_BASE_URL", "https://apidadosabertos.saude.gov.br"),
        "page_size": int(os.environ.get("API_PAGE_SIZE", "100")),
        "max_retries": int(os.environ.get("API_MAX_RETRIES", "5")),
    }
