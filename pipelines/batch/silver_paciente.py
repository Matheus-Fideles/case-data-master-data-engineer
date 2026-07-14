"""Silver job: bronze/oltp_paciente → s3://silver/paciente/ (Delta Lake).

Aplica mascaramento LGPD conforme ADR-0004:
  - cpf        → SHA-256 + salt → id_paciente_hash (determinístico para joins)
  - nome       → suprimido (sem valor analítico direto)
  - data_nascimento → generalizado para ano_nascimento (IntegerType)
  - cep        → truncado para 3 primeiros dígitos (microrregião)
  - email      → suprimido (NULL)
  - telefone   → suprimido (NULL)

Após a transformação, validate_no_pii() garante que nenhuma coluna PII
sobreviveu — falha aqui bloqueia escrita no Silver.

Args:
  --snapshot_date  YYYYMMDD
  --batch_id       DAG run_id
  --input_path     override (opcional)
  --output_path    override (opcional)
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from pipelines.batch.silver_base import SilverJob
from pipelines.common.masking import mask_paciente, validate_no_pii
from pipelines.common.spark import build_spark

# Versão do algoritmo de mascaramento — rastreável em cada linha Silver.
MASKING_VERSION = "masking-1.0.0"

# Colunas PII que devem desaparecer após transform().
_PII_COLS = ["cpf", "nome", "email", "telefone"]


class PacienteSilverJob(SilverJob):
    source_name = "silver_paciente"
    partition_cols = ["snapshot_date"]

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. hash CPF → id_paciente_hash; remove cpf original
        df = mask_paciente(df, cpf_col="cpf", output_col="id_paciente_hash")

        # 2. generaliza data de nascimento → apenas ano
        df = df.withColumn(
            "ano_nascimento",
            F.year(F.to_date(F.col("data_nascimento"))).cast(IntegerType()),
        ).drop("data_nascimento", "nome")

        # 3. trunca CEP para 3 primeiros dígitos (microrregião)
        df = df.withColumn("cep_regiao", F.substring(F.col("cep"), 1, 3)).drop("cep")

        # 4. suprime email e telefone
        df = df.drop("email", "telefone")

        # 5. adiciona metadados de mascaramento
        df = df.withColumn("masking_version", F.lit(MASKING_VERSION))
        df = df.withColumn("masked_at", F.current_timestamp())

        # 6. guarda: validate_no_pii levanta ValueError se qualquer PII sobreviveu
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
