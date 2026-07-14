"""
Base utilities for REST API extraction.
Handles pagination, retry, S3 write, and offline mode.
ADR 0008: extraction is pure Python — no Spark JVM overhead here.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.client import Config
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

API_BASE = os.getenv("API_BASE_URL", "https://apidadosabertos.saude.gov.br")
PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "100"))
MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
OFFLINE_MODE = os.getenv("OFFLINE_MODE", "0") == "1"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "landing")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@retry(
    retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(MAX_RETRIES),
    reraise=True,
)
def _get(url: str, params: dict) -> requests.Response:
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 10))
        import time
        time.sleep(retry_after)
        resp.raise_for_status()
    resp.raise_for_status()
    return resp


def fetch_paginated(
    path: str,
    data_key: str,
    params: dict | None = None,
    offset_param: str = "offset",
    page_size: int | None = None,
) -> list[dict]:
    """Fetch all pages from a paginated API endpoint.

    Stops when a page returns fewer items than page_size.
    """
    size = page_size or PAGE_SIZE
    base_params = dict(params or {})
    base_params["limit"] = size
    results: list[dict] = []
    offset = 0

    while True:
        base_params[offset_param] = offset
        url = f"{API_BASE}{path}"
        log.info("GET %s offset=%d", url, offset)
        data = _get(url, base_params).json()
        page = data.get(data_key, [])
        if not isinstance(page, list):
            page = []
        results.extend(page)
        log.info("  fetched %d records (total so far: %d)", len(page), len(results))
        if len(page) < size:
            break
        offset += size

    return results


def fetch_paginated_by_page(
    path: str,
    data_key: str,
    params: dict | None = None,
    page_size: int | None = None,
) -> list[dict]:
    """Pagination using page/size parameters (vacinacao PNI pattern)."""
    size = page_size or PAGE_SIZE
    base_params = dict(params or {})
    base_params["size"] = size
    results: list[dict] = []
    page = 0

    while True:
        base_params["page"] = page
        url = f"{API_BASE}{path}"
        log.info("GET %s page=%d", url, page)
        data = _get(url, base_params).json()
        items = data.get(data_key, [])
        if not isinstance(items, list):
            items = []
        results.extend(items)
        log.info("  fetched %d records (total so far: %d)", len(items), len(results))
        if len(items) < size:
            break
        page += 1

    return results


def load_from_cache(fonte: str, data_ref: str) -> list[dict]:
    """Load records from local cache (OFFLINE_MODE=1)."""
    cache_dir = Path("data/raw") / fonte / data_ref
    records: list[dict] = []
    for f in sorted(cache_dir.glob("*.json")):
        with f.open() as fh:
            data = json.load(fh)
            if isinstance(data, list):
                records.extend(data)
            else:
                records.append(data)
    log.info("Loaded %d records from cache %s", len(records), cache_dir)
    return records


def save_to_cache(records: list[dict], fonte: str, data_ref: str, filename: str = "data.json") -> Path:
    """Save records to local cache directory."""
    cache_dir = Path("data/raw") / fonte / data_ref
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / filename
    with out.open("w") as fh:
        json.dump(records, fh, ensure_ascii=False, default=str)
    log.info("Saved %d records to %s", len(records), out)
    return out


def write_ndjson_to_landing(
    records: list[dict],
    fonte: str,
    data_ref: str,
    source_url: str,
    filename: str = "part-00000.json",
) -> str:
    """Write records as NDJSON to MinIO landing bucket. Returns S3 key."""
    s3_key = f"{fonte}/{data_ref}/{filename}"
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in records)
    _s3_client().put_object(
        Bucket=BUCKET_LANDING,
        Key=s3_key,
        Body=ndjson.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={"source_url": source_url[:1024]},
    )
    log.info("Written %d records to s3://%s/%s", len(records), BUCKET_LANDING, s3_key)
    return f"s3a://{BUCKET_LANDING}/{s3_key}"


def extract(
    fonte: str,
    data_ref: str,
    fetch_fn,
    source_url: str,
) -> dict[str, Any]:
    """
    Main extraction entry point. Respects OFFLINE_MODE.
    Returns {"row_count": int, "output_path": str, "source_url": str}.
    """
    if OFFLINE_MODE:
        records = load_from_cache(fonte, data_ref)
    else:
        records = fetch_fn()
        save_to_cache(records, fonte, data_ref)

    assert len(records) > 0, f"API retornou vazio para {fonte}/{data_ref}"

    output_path = write_ndjson_to_landing(records, fonte, data_ref, source_url)
    return {"row_count": len(records), "output_path": output_path, "source_url": source_url}
