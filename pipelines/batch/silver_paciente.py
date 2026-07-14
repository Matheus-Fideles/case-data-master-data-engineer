"""Silver job: bronze/oltp_paciente → s3://silver/paciente/ (Delta Lake).

Applies LGPD masking per ADR-0004:
  - cpf        → SHA-256 + salt → id_paciente_hash (deterministic for joins)
  - nome       → suppressed (no direct analytical value)
  - data_nascimento → generalized to ano_nascimento (IntegerType)
  - cep        → truncated to first 3 digits (micro-region)
  - email      → suppressed (NULL)
  - telefone   → suppressed (NULL)

After transformation, validate_no_pii() ensures no PII column survived
— failure here blocks writing to Silver.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
  --input_path     override (optional)
  --output_path    override (optional)
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from pipelines.batch.silver_base import SilverJob
from pipelines.common.masking import mask_paciente, validate_no_pii
from pipelines.common.spark import build_spark

# Masking algorithm version — traceable on each Silver row.
MASKING_VERSION = "masking-1.0.0"

# PII columns that must disappear after transform().
_PII_COLS = ["cpf", "nome", "email", "telefone"]


class PacienteSilverJob(SilverJob):
    source_name = "silver_paciente"
    partition_cols = ["snapshot_date"]

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. hash CPF → id_paciente_hash; remove cpf original
        df = mask_paciente(df, cpf_col="cpf", output_col="id_paciente_hash")

        # 2. generalize date of birth → year only
        df = df.withColumn(
            "ano_nascimento",
            F.year(F.to_date(F.col("data_nascimento"))).cast(IntegerType()),
        ).drop("data_nascimento", "nome")

        # 3. truncate CEP to first 3 digits (micro-region)
        df = df.withColumn("cep_regiao", F.substring(F.col("cep"), 1, 3)).drop("cep")

        # 4. suppress email and telefone
        df = df.drop("email", "telefone")

        # 5. add masking metadata
        df = df.withColumn("masking_version", F.lit(MASKING_VERSION))
        df = df.withColumn("masked_at", F.current_timestamp())

        # 6. guard: validate_no_pii raises ValueError if any PII survived
        validate_no_pii(df, pii_cols=_PII_COLS + ["data_nascimento"])

        return df


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot_date", required=True)
    p.add_argument("--batch_id", default="manual")
    p.add_argument("--input_path", default=None)
    p.add_argument("--output_path", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    input_path = args.input_path or "s3a://bronze/oltp_paciente/"
    output_path = args.output_path or "s3a://silver/paciente/"

    spark = build_spark("silver_paciente")
    spark.sparkContext.setLogLevel("WARN")
    PacienteSilverJob().run(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        filter_col="snapshot_date",
        filter_val=args.snapshot_date,
        batch_id=args.batch_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()
