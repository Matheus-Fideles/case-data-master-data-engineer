"""Bronze job: landing/sim → s3://bronze/sim_obitos/ (Delta Lake).

Particionado por ano_part (YYYY).

Args:
  --ano         YYYY
  --batch_id    DAG run_id
  --input_path  override (opcional)
  --output_path override (opcional)
"""

from __future__ import annotations

import argparse

from pyspark.sql.types import StringType, StructField, StructType

from pipelines.batch.bronze_base import BronzeJob
from pipelines.common.spark import build_spark

_SCHEMA = StructType(
    [
        StructField("contador", StringType(), True),
        StructField("causabas", StringType(), True),
        StructField("dtobito", StringType(), True),
        StructField("dtnasc", StringType(), True),
        StructField("idade", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("racacor", StringType(), True),
        StructField("codmunocor", StringType(), True),
        StructField("codmunres", StringType(), True),
        StructField("lococor", StringType(), True),
        StructField("assistmed", StringType(), True),
        StructField("tipobito", StringType(), True),
        StructField("causabas_o", StringType(), True),
        StructField("linhaa", StringType(), True),
        StructField("linhab", StringType(), True),
        StructField("linhac", StringType(), True),
        StructField("linhad", StringType(), True),
        StructField("linhaii", StringType(), True),
        StructField("altcausa", StringType(), True),
        StructField("circobito", StringType(), True),
        StructField("obitoparto", StringType(), True),
        StructField("obitograv", StringType(), True),
        StructField("gravidez", StringType(), True),
        StructField("gestacao", StringType(), True),
        StructField("escmae2010", StringType(), True),
        StructField("esc2010", StringType(), True),
        StructField("necropsia", StringType(), True),
        StructField("origem", StringType(), True),
        StructField("numerolote", StringType(), True),
        StructField("versaosist", StringType(), True),
    ]
)


class SimObitosBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_sim_obitos"
    partition_col = "ano_part"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ano", required=True)
    p.add_argument("--batch_id", default="manual")
    p.add_argument("--input_path", default=None)
    p.add_argument("--output_path", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    input_path = args.input_path or f"s3a://landing/sim/{args.ano}/"
    output_path = args.output_path or "s3a://bronze/sim_obitos/"

    spark = build_spark("bronze_sim_obitos")
    spark.sparkContext.setLogLevel("WARN")
    SimObitosBronzeJob().run(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        partition_val=args.ano,
        batch_id=args.batch_id,
        source_url=input_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
