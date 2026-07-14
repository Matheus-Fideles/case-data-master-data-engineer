"""Helper to create a local SparkSession for maintenance DAGs."""

from __future__ import annotations

import os


def make_local_spark(app_name: str = "airflow_maint"):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
        .config(
            "spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "2g")
        .master("local[2]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


BRONZE_TABLES = [
    "s3a://bronze/arboviroses/dengue/",
    "s3a://bronze/arboviroses/zika/",
    "s3a://bronze/arboviroses/chikungunya/",
    "s3a://bronze/sim_obitos/",
    "s3a://bronze/vacinacao_pni/",
    "s3a://bronze/cnes/",
    "s3a://bronze/municipios/",
    "s3a://bronze/oltp_paciente/",
    "s3a://bronze/atendimentos/",
]
