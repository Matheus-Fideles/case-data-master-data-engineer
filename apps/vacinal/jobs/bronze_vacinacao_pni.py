"""Bronze job: landing/vacinal/vacinacao_pni → s3://bronze/vacinal/vacinacao_pni/ (Delta Lake).

Partitioned by year_month (YYYYMM). The partition is DERIVED from the data_vacina field
(format YYYY-MM-DD), not received as a literal — hence the override of add_partition().

codigo_paciente arrives pre-hashed by the Ministry — no PII in this table.

Args:
  --ano_mes     YYYYMM (used as replaceWhere and partition fallback)
  --batch_id    DAG run_id
  --input_path  override (opcional)
  --output_path override (opcional)
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.shared.bronze_base import BronzeJob
from apps.shared.spark import build_spark

_SCHEMA = StructType(
    [
        StructField("codigo_documento", StringType(), True),
        StructField("codigo_paciente", StringType(), True),
        StructField("codigo_vacina", StringType(), True),
        StructField("sigla_vacina", StringType(), True),
        StructField("descricao_vacina", StringType(), True),
        StructField("data_vacina", StringType(), True),
        StructField("codigo_dose_vacina", StringType(), True),
        StructField("descricao_dose_vacina", StringType(), True),
        StructField("codigo_cnes_estabelecimento", StringType(), True),
        StructField("codigo_municipio_estabelecimento", StringType(), True),
        StructField("sigla_uf_estabelecimento", StringType(), True),
        StructField("nome_municipio_estabelecimento", StringType(), True),
        StructField("numero_idade_paciente", StringType(), True),
        StructField("nome_raca_cor_paciente", StringType(), True),
        StructField("codigo_raca_cor_paciente", StringType(), True),
        StructField("tipo_sexo_paciente", StringType(), True),
        StructField("codigo_municipio_paciente", StringType(), True),
        StructField("sigla_uf_paciente", StringType(), True),
        StructField("descricao_estrategia_vacinacao", StringType(), True),
        StructField("codigo_estrategia_vacinacao", StringType(), True),
        StructField("codigo_vacina_fabricante", StringType(), True),
        StructField("descricao_vacina_fabricante", StringType(), True),
        StructField("status_documento", StringType(), True),
        StructField("data_entrada_rnds", StringType(), True),
    ]
)


class VacinacaoPniBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_vacinacao_pni"
    partition_col = "ano_mes"

    def add_partition(self, df: DataFrame, partition_val: str) -> DataFrame:
        """Derives year_month from data_vacina (YYYY-MM-DD → YYYYMM).

        Override of the base hook: the partition is not an external literal,
        it is computed from a field in the source data itself.
        """
        return df.withColumn(
            self.partition_col,
            F.when(
                F.col("data_vacina").rlike(r"^\d{4}-\d{2}"),
                F.regexp_replace(F.col("data_vacina").substr(1, 7), "-", ""),
            ).otherwise(F.lit(partition_val)),
        )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ano_mes", required=True)
    p.add_argument("--batch_id", default="manual")
    p.add_argument("--input_path", default=None)
    p.add_argument("--output_path", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    ano = args.ano_mes[:4]
    input_path = args.input_path or f"s3a://landing/vacinal/vacinacao_pni/{ano}/"
    output_path = args.output_path or "s3a://bronze/vacinal/vacinacao_pni/"

    spark = build_spark("bronze_vacinacao_pni")
    spark.sparkContext.setLogLevel("WARN")
    VacinacaoPniBronzeJob().run(
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
