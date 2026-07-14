"""Factory: composes adapters with the application service.

Single place that reads environment variables and instantiates infrastructure.
Individual extractors call `make_extraction_service()` and receive
a ready ExtractionService — without direct coupling to boto3 or the filesystem.
"""

from __future__ import annotations

import os

from pipelines.extraction.adapters.local_cache import LocalCacheAdapter
from pipelines.extraction.adapters.s3_landing import S3LandingAdapter
from pipelines.extraction.service import ExtractionService


def make_extraction_service() -> ExtractionService:
    """Composes and returns an ExtractionService configured from the environment."""
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
    """Returns pagination configuration read from the environment."""
    return {
        "base_url": os.environ.get("API_BASE_URL", "https://apidadosabertos.saude.gov.br"),
        "page_size": int(os.environ.get("API_PAGE_SIZE", "100")),
        "max_retries": int(os.environ.get("API_MAX_RETRIES", "5")),
    }
