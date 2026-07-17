"""Centralised SparkSession factory with Delta + S3A + OpenLineage.

Eliminates build_spark() duplication across all batch jobs.
Configuration injected via environment variables (DIP).

OpenLineage (data lineage):
  When OPENLINEAGE_URL is set, activates the automatic listener
  `io.openlineage.spark.agent.OpenLineageSparkListener` which captures
  all Delta and S3 inputs/outputs without changes in the job code.
  See: docs/05-observabilidade-sre.md — section "Lineage"
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_OL_URL = os.environ.get("OPENLINEAGE_URL", "")
_OL_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "batch")


def build_spark(app_name: str) -> SparkSession:
    """Creates a SparkSession configured for Delta Lake + MinIO (S3A).

    Activates the OpenLineage listener automatically if OPENLINEAGE_URL is set.
    All credentials are injected via env vars — no hardcoding.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    if _OL_URL:
        builder = _configure_openlineage(builder, app_name)

    return configure_spark_with_delta_pip(builder).getOrCreate()


def _configure_openlineage(builder, app_name: str):
    """Adds the OpenLineage listener to the SparkSession."""
    return (
        builder.config(
            "spark.extraListeners",
            "io.openlineage.spark.agent.OpenLineageSparkListener",
        )
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", _OL_URL)
        .config("spark.openlineage.namespace", _OL_NAMESPACE)
        .config("spark.openlineage.appName", app_name)
        .config("spark.openlineage.facets.spark_unknown.disabled", "false")
        .config("spark.openlineage.facets.spark.logicalPlan.disabled", "false")
    )
