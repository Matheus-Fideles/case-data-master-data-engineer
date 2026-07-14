"""Bronze job: landing/oltp_paciente → s3://bronze/oltp_paciente/ (Delta Lake).

⚠️  CONTÉM PII — particionado por snapshot_date (YYYYMMDD).
Dado mascarado apenas no Silver (ADR-0004). Acesso Bronze restrito
ao role gold_engineer via RBAC Postgres + MinIO bucket policy.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
  --input_path     override (opcional)
  --output_path    override (opcional)
"""
from __future__ import annotations

import argparse

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.batch.bronze_base import BronzeJob
from pipelines.common.spark import build_spark

# Campos PII presentes intencionalmente — Bronze é a única camada raw.
# Silver mascara antes de qualquer propagação (ADR-0004).
_SCHEMA = StructType([
    StructField("id_paciente",           LongType(),      False),
    StructField("cpf",                   StringType(),    False),  # PII
    StructField("nome",                  StringType(),    False),  # PII
    StructField("data_nascimento",       StringType(),    False),  # PII — string; cast no Silver
    StructField("sexo",                  StringType(),    False),
    StructField("cep",                   StringType(),    False),  # PII
    StructField("municipio_codigo_ibge", StringType(),    False),
    StructField("email",                 StringType(),    True),   # PII
    StructField("telefone",              StringType(),    True),   # PII
    StructField("created_at",            TimestampType(), False),
    StructField("updated_at",            TimestampType(), False),
])


class OltpPacienteBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_oltp_paciente"
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
    input_path = args.input_path or f"s3a://landing/oltp_paciente/{args.snapshot_date}/"
    output_path = args.output_path or "s3a://bronze/oltp_paciente/"

    spark = build_spark("bronze_oltp_paciente")
    spark.sparkContext.setLogLevel("WARN")
    OltpPacienteBronzeJob().run(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        partition_val=args.snapshot_date,
        batch_id=args.batch_id,
        source_url=f"postgres://oltp/paciente?snapshot_date={args.snapshot_date}",
    )
    spark.stop()


if __name__ == "__main__":
    main()
