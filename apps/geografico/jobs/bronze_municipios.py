"""Bronze job: landing/geografico/municipios → s3://bronze/geografico/municipios/ (Delta Lake).

Particionado por snapshot_date (YYYYMMDD).

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
  --input_path     override (opcional)
  --output_path    override (opcional)
"""

from __future__ import annotations

import argparse

from pyspark.sql.types import StringType, StructField, StructType

from apps.shared.bronze_base import BronzeJob
from apps.shared.spark import build_spark

_SCHEMA = StructType(
    [
        StructField("codigo_regiao_pais", StringType(), True),
        StructField("regiao_pais", StringType(), True),
        StructField("codigo_uf", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("codigo_macrorregiao_saude", StringType(), True),
        StructField("macrorregiao_saude", StringType(), True),
        StructField("codigo_regiao_saude", StringType(), True),
        StructField("regiao_saude", StringType(), True),
        StructField("codigo_municipio", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("populacao_estimada_ibge_2022", StringType(), True),
    ]
)


class MunicipiosBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_municipios"
    partition_col = "snapshot_date"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot_date", required=True)
    p.add_argument("--batch_id", default="manual")
    p.add_argument("--input_path", default=None)
    p.add_argument("--output_path", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    input_path = args.input_path or f"s3a://landing/geografico/municipios/{args.snapshot_date}/"
    output_path = args.output_path or "s3a://bronze/geografico/municipios/"

    spark = build_spark("bronze_municipios")
    spark.sparkContext.setLogLevel("WARN")
    MunicipiosBronzeJob().run(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        partition_val=args.snapshot_date,
        batch_id=args.batch_id,
        source_url=input_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
