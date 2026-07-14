"""Factory centralizada para SparkSession com Delta + S3A.

Elimina duplicação de build_spark() em todos os jobs batch.
Configurações injetadas via variáveis de ambiente (DIP).
"""
from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    """Cria SparkSession configurada para Delta Lake + MinIO (S3A).

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
    return configure_spark_with_delta_pip(builder).getOrCreate()
