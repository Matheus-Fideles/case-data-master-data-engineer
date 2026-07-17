"""Strategy: carrega dim_municipio (SCD Type 1)."""

from __future__ import annotations

from pyspark.sql import functions as F

from apps.shared.gold_base import DimLoader


class MunicipioDimLoader(DimLoader):
    def load(self, snapshot_date: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/geografico/municipio/")
            .filter(F.col("snapshot_date") == snapshot_date)
            .select(
                F.col("codigo_municipio"),
                F.col("nome_municipio"),
                F.col("sigla_uf"),
                F.col("regiao_saude").alias("nome_regiao_saude"),
                F.col("macrorregiao_saude").alias("nome_macro_saude"),
                F.col("codigo_uf"),
                F.col("populacao_estimada_ibge_2022").cast("int").alias("populacao_2022"),
            )
            .dropDuplicates(["codigo_municipio"])
            .withColumn("_batch_id", F.lit(snapshot_date))
        )
        return self._write(df, "s3a://gold/geografico/dim_municipio/")
