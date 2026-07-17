"""Bronze job: landing/oltp/paciente → s3://bronze/oltp/paciente/ (Delta Lake).

⚠️  CONTAINS PII — partitioned by snapshot_date (YYYYMMDD).
Data masked in Silver only (ADR-0004). Bronze access restricted
to the gold_engineer role via Postgres RBAC + MinIO bucket policy.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
  --input_path     override (optional)
  --output_path    override (optional)
"""

from __future__ import annotations

import argparse

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from apps.shared.bronze_base import BronzeJob
from apps.shared.spark import build_spark

# PII fields present intentionally — Bronze is the only raw layer.
# Silver masks before any propagation (ADR-0004).
_SCHEMA = StructType(
    [
        StructField("id_paciente", LongType(), False),
        StructField("cpf", StringType(), False),  # PII
        StructField("nome", StringType(), False),  # PII
        StructField("data_nascimento", StringType(), False),  # PII — string; cast no Silver
        StructField("sexo", StringType(), False),
        StructField("cep", StringType(), False),  # PII
        StructField("municipio_codigo_ibge", StringType(), False),
        StructField("email", StringType(), True),  # PII
        StructField("telefone", StringType(), True),  # PII
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)


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
    input_path = args.input_path or f"s3a://landing/oltp/paciente/{args.snapshot_date}/"
    output_path = args.output_path or "s3a://bronze/oltp/paciente/"

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
