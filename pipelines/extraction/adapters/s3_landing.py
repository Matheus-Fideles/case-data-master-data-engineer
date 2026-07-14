"""Adapter: LandingStoragePort → MinIO/S3 (boto3).

Implements the output port. Domain logic never imports boto3 directly.
"""

from __future__ import annotations

import json
import logging

import boto3
from botocore.client import Config

log = logging.getLogger(__name__)


class S3LandingAdapter:
    """Implements LandingStoragePort by writing NDJSON to MinIO via S3A."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "landing",
    ) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def write(
        self,
        records: list[dict],
        fonte: str,
        data_ref: str,
        source_url: str,
        filename: str = "part-00000.json",
    ) -> str:
        s3_key = f"{fonte}/{data_ref}/{filename}"
        ndjson = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in records)
        self._client.put_object(
            Bucket=self._bucket,
            Key=s3_key,
            Body=ndjson.encode("utf-8"),
            ContentType="application/x-ndjson",
            Metadata={"source_url": source_url[:1024]},
        )
        log.info("Written %d records to s3://%s/%s", len(records), self._bucket, s3_key)
        return f"s3a://{self._bucket}/{s3_key}"
