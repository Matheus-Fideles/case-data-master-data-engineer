"""Silver job: bronze/epidemiologico/<agravo> → s3://silver/epidemiologico/notificacao/ (Delta Lake).

Merges dengue, zika and chikungunya into a single notification dataset.
Particionado por (agravo, ano_mes). replaceWhere usa ambas as colunas.

Args:
  --agravo   dengue | zika | chikungunya
  --ano_mes  YYYYMM
  --batch_id DAG run_id
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from apps.shared.silver_base import SilverJob
from apps.shared.spark import build_spark


class NotificacaoSilverJob(SilverJob):
    source_name = "silver_notificacao"
    partition_cols = ["agravo", "ano_mes"]

    def __init__(self, agravo: str) -> None:
        self._agravo = agravo

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("agravo", F.lit(self._agravo))
            .withColumn("dt_notific", F.to_date(F.col("dt_notific"), "yyyy-MM-dd"))
            .withColumn("dt_sin_pri", F.to_date(F.col("dt_sin_pri"), "yyyy-MM-dd"))
            .withColumn("dt_obito", F.to_date(F.col("dt_obito"), "yyyy-MM-dd"))
            .withColumn("dt_encerra", F.to_date(F.col("dt_encerra"), "yyyy-MM-dd"))
            .withColumn("nu_idade_n", F.col("nu_idade_n").cast(IntegerType()))
            .withColumn("nu_ano", F.col("nu_ano").cast(IntegerType()))
        )

    def replace_condition(self, filter_col: str, filter_val: str) -> str:
        return f"agravo = '{self._agravo}' AND {filter_col} = '{filter_val}'"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--agravo", required=True, choices=["dengue", "zika", "chikungunya"])
    p.add_argument("--ano_mes", required=True)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("silver_notificacao")
    spark.sparkContext.setLogLevel("WARN")
    NotificacaoSilverJob(agravo=args.agravo).run(
        spark=spark,
        input_path=f"s3a://bronze/epidemiologico/{args.agravo}/",
        output_path="s3a://silver/epidemiologico/notificacao/",
        filter_col="ano_mes",
        filter_val=args.ano_mes,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
