"""Factory centralizada para SparkSession com Delta + S3A + OpenLineage.

Elimina duplicação de build_spark() em todos os jobs batch.
Configurações injetadas via variáveis de ambiente (DIP).

OpenLineage (rastreabilidade de dados):
  Se OPENLINEAGE_URL estiver definida, ativa o listener automático
  `io.openlineage.spark.agent.OpenLineageSparkListener` que captura
  todos os inputs/outputs Delta e S3 sem alteração no código dos jobs.
  Ver: docs/observability.md — seção "Lineage"
"""
from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_OL_URL       = os.environ.get("OPENLINEAGE_URL", "")
_OL_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "batch")


def build_spark(app_name: str) -> SparkSession:
    """Cria SparkSession configurada para Delta Lake + MinIO (S3A).

    Ativa OpenLineage listener automaticamente se OPENLINEAGE_URL estiver definida.
    Todas as credenciais são injetadas via env vars — sem hardcode.
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
    """Adiciona o listener OpenLineage à SparkSession."""
    return (
        builder
        .config(
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
