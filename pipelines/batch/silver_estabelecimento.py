"""Silver job: bronze/cnes → s3://silver/estabelecimento/ (Delta Lake).

Normalizes CNES healthcare establishments. Partitioned by snapshot_date.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from pipelines.batch.silver_base import SilverJob
from pipelines.common.spark import build_spark


class EstabelecimentoSilverJob(SilverJob):
    source_name = "silver_estabelecimento"
    partition_cols = ["snapshot_date"]

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("codigo_cnes", F.col("codigo_cnes").cast(IntegerType()))
            .withColumn("codigo_municipio", F.col("codigo_municipio").cast(IntegerType()))
            .withColumn("codigo_uf", F.col("codigo_uf").cast(IntegerType()))
            .withColumnRenamed("nome_fantasia", "nome_fantasia")
            .withColumnRenamed("nome_razao_social", "razao_social")
            .withColumnRenamed("tipo_gestao", "tipo_gestao")
        )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot_date", required=True)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("silver_estabelecimento")
    spark.sparkContext.setLogLevel("WARN")
    EstabelecimentoSilverJob().run(
        spark=spark,
        input_path="s3a://bronze/cnes/",
        output_path="s3a://silver/estabelecimento/",
        filter_col="snapshot_date",
        filter_val=args.snapshot_date,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
