"""Silver job: bronze/vacinacao_pni → s3://silver/vacinacao_pni/ (Delta Lake).

Normalizes types. codigo_paciente arrives pre-hashed by the Ministry — no PII.
Particionado por ano_mes.

Args:
  --ano_mes  YYYYMM
  --batch_id DAG run_id
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from pipelines.batch.silver_base import SilverJob
from pipelines.common.spark import build_spark


class VacinacaoPniSilverJob(SilverJob):
    source_name = "silver_vacinacao_pni"
    partition_cols = ["ano_mes"]

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("data_vacina", F.to_date(F.col("data_vacina"), "yyyy-MM-dd"))
            .withColumn(
                "codigo_municipio_paciente",
                F.col("codigo_municipio_paciente").cast(IntegerType()),
            )
            .withColumn(
                "codigo_cnes_estabelecimento",
                F.col("codigo_cnes_estabelecimento").cast(IntegerType()),
            )
            .withColumn("numero_idade_paciente", F.col("numero_idade_paciente").cast(IntegerType()))
        )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ano_mes", required=True)
    p.add_argument("--batch_id", default="manual")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("silver_vacinacao_pni")
    spark.sparkContext.setLogLevel("WARN")
    VacinacaoPniSilverJob().run(
        spark=spark,
        input_path="s3a://bronze/vacinacao_pni/",
        output_path="s3a://silver/vacinacao_pni/",
        filter_col="ano_mes",
        filter_val=args.ano_mes,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
