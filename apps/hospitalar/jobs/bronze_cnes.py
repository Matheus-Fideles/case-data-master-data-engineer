"""Bronze job: landing/hospitalar/cnes → s3://bronze/hospitalar/cnes/ (Delta Lake).

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
        StructField("codigo_cnes", StringType(), True),
        StructField("nome_razao_social", StringType(), True),
        StructField("nome_fantasia", StringType(), True),
        StructField("natureza_organizacao_entidade", StringType(), True),
        StructField("tipo_gestao", StringType(), True),
        StructField("descricao_nivel_hierarquia", StringType(), True),
        StructField("descricao_esfera_administrativa", StringType(), True),
        StructField("codigo_tipo_unidade", StringType(), True),
        StructField("codigo_cep_estabelecimento", StringType(), True),
        StructField("endereco_estabelecimento", StringType(), True),
        StructField("numero_estabelecimento", StringType(), True),
        StructField("bairro_estabelecimento", StringType(), True),
        StructField("latitude_estabelecimento_decimo_grau", StringType(), True),
        StructField("longitude_estabelecimento_decimo_grau", StringType(), True),
        StructField("endereco_email_estabelecimento", StringType(), True),
        StructField("numero_cnpj", StringType(), True),
        StructField("codigo_municipio", StringType(), True),
        StructField("codigo_uf", StringType(), True),
        StructField("estabelecimento_possui_atendimento_hospitalar", StringType(), True),
        StructField("estabelecimento_possui_centro_cirurgico", StringType(), True),
        StructField("estabelecimento_possui_servico_apoio", StringType(), True),
        StructField("estabelecimento_possui_atendimento_ambulatorial", StringType(), True),
        StructField("data_atualizacao", StringType(), True),
        StructField("codigo_estabelecimento_saude", StringType(), True),
    ]
)


class CnesBronzeJob(BronzeJob):
    schema = _SCHEMA
    source_name = "bronze_cnes"
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
    input_path = args.input_path or f"s3a://landing/hospitalar/cnes/{args.snapshot_date}/"
    output_path = args.output_path or "s3a://bronze/hospitalar/cnes/"

    spark = build_spark("bronze_cnes")
    spark.sparkContext.setLogLevel("WARN")
    CnesBronzeJob().run(
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
