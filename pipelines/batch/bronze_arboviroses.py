"""Bronze job: landing/arboviroses → s3://bronze/<agravo>/ (Delta Lake).

Handles dengue, zika e chikungunya com schema superset unificado.

Args:
  --agravo      dengue | zika | chikungunya
  --ano_mes     YYYYMM  (partition key)
  --batch_id    DAG run_id
  --input_path  override (opcional)
  --output_path override (opcional)
"""
from __future__ import annotations

import argparse

from pyspark.sql.types import StringType, StructField, StructType

from pipelines.batch.bronze_base import BronzeJob
from pipelines.common.spark import build_spark

_SCHEMA = StructType([
    StructField("tp_not", StringType(), True),
    StructField("id_agravo", StringType(), True),
    StructField("dt_notific", StringType(), True),
    StructField("sem_not", StringType(), True),
    StructField("nu_ano", StringType(), True),
    StructField("sg_uf_not", StringType(), True),
    StructField("id_municip", StringType(), True),
    StructField("id_regiona", StringType(), True),
    StructField("id_unidade", StringType(), True),
    StructField("dt_sin_pri", StringType(), True),
    StructField("sem_pri", StringType(), True),
    StructField("nu_idade_n", StringType(), True),
    StructField("cs_sexo", StringType(), True),
    StructField("cs_gestant", StringType(), True),
    StructField("cs_raca", StringType(), True),
    StructField("cs_escol_n", StringType(), True),
    StructField("sg_uf", StringType(), True),
    StructField("id_mn_resi", StringType(), True),
    StructField("id_rg_resi", StringType(), True),
    StructField("id_pais", StringType(), True),
    StructField("classi_fin", StringType(), True),
    StructField("criterio", StringType(), True),
    StructField("evolucao", StringType(), True),
    StructField("dt_obito", StringType(), True),
    StructField("dt_encerra", StringType(), True),
    StructField("hospitaliz", StringType(), True),
    StructField("dt_interna", StringType(), True),
    StructField("ano_nasc", StringType(), True),
    StructField("febre", StringType(), True),
    StructField("mialgia", StringType(), True),
    StructField("cefaleia", StringType(), True),
    StructField("exantema", StringType(), True),
    StructField("arquivo", StringType(), True),
])


class ArbovirosesBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_arboviroses"
    partition_col = "ano_mes"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--agravo", required=True, choices=["dengue", "zika", "chikungunya"])
    p.add_argument("--ano_mes", required=True)
    p.add_argument("--batch_id", default="manual")
    p.add_argument("--input_path", default=None)
    p.add_argument("--output_path", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    ano = args.ano_mes[:4]
    input_path = args.input_path or f"s3a://landing/{args.agravo}/{ano}/"
    output_path = args.output_path or f"s3a://bronze/{args.agravo}/"

    spark = build_spark("bronze_arboviroses")
    spark.sparkContext.setLogLevel("WARN")
    ArbovirosesBronzeJob().run(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        partition_val=args.ano_mes,
        batch_id=args.batch_id,
        source_url=input_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
