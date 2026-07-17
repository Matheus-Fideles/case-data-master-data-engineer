"""Strategy: carrega dim_vacina (SCD Type 1)."""

from __future__ import annotations

from pyspark.sql import functions as F

from apps.shared.gold_base import DimLoader


class VacinaDimLoader(DimLoader):
    def load(self, ano_mes: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/vacinal/vacinacao_pni/")
            .filter(F.col("ano_mes") == ano_mes)
            .select(
                F.col("codigo_vacina").cast("int"),
                F.col("descricao_vacina").alias("nome_vacina"),
                F.col("sigla_vacina"),
                F.col("descricao_estrategia_vacinacao").alias("estrategia"),
            )
            .dropDuplicates(["codigo_vacina"])
        )
        return self._write(df, "s3a://gold/vacinal/dim_vacina/")
