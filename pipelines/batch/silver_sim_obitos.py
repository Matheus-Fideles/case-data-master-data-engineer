"""Silver job: bronze/sim_obitos → s3://silver/sim_obitos/ (Delta Lake).

Normalizes types and applies defensive PII masking (LGPD).
Particionado por ano_part.

Args:
  --ano      YYYY
  --batch_id DAG run_id
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from pipelines.batch.silver_base import SilverJob
from pipelines.common.spark import build_spark

_PII_COLS = ("nome", "cpf", "nome_mae", "nome_pai")


class SimObitosSilverJob(SilverJob):
    source_name = "silver_sim_obitos"
    partition_cols = ["ano_part"]

    def __init__(self, salt: str) -> None:
        self._salt = salt

    def transform(self, df: DataFrame) -> DataFrame:
        df = (
            df
            .withColumn("dtobito", F.to_date(F.col("dtobito"), "yyyyMMdd"))
            .withColumn("dtnasc",  F.to_date(F.col("dtnasc"),  "yyyyMMdd"))
            .withColumn("codmunocor", F.col("codmunocor").cast(IntegerType()))
            .withColumn("idade", F.col("idade").cast(IntegerType()))
        )
        for col in _PII_COLS:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.sha2(F.concat(F.lit(f"{self._salt}:"), F.col(col)), 256),
                )
        return df


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ano", required=True)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    salt = os.environ.get("PII_SALT", "santander-case-2024")
    spark = build_spark("silver_sim_obitos")
    spark.sparkContext.setLogLevel("WARN")
    SimObitosSilverJob(salt=salt).run(
        spark=spark,
        input_path="s3a://bronze/sim_obitos/",
        output_path="s3a://silver/sim_obitos/",
        filter_col="ano_part",
        filter_val=args.ano,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
