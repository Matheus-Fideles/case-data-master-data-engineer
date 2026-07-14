"""Silver job: bronze/municipios → s3://silver/municipio/ (Delta Lake).

Normalizes and types the municipality table. Partitioned by snapshot_date.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.batch.silver_base import SilverJob
from pipelines.common.spark import build_spark


class MunicipioSilverJob(SilverJob):
    source_name = "silver_municipio"
    partition_cols = ["snapshot_date"]

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("codigo_municipio", F.col("codigo_municipio").cast("int"))
            .withColumn("codigo_uf", F.col("codigo_uf").cast("int"))
            .withColumnRenamed("municipio", "nome_municipio")
            .withColumnRenamed("uf", "sigla_uf")
        )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot_date", required=True)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("silver_municipio")
    spark.sparkContext.setLogLevel("WARN")
    MunicipioSilverJob().run(
        spark=spark,
        input_path="s3a://bronze/municipios/",
        output_path="s3a://silver/municipio/",
        filter_col="snapshot_date",
        filter_val=args.snapshot_date,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
