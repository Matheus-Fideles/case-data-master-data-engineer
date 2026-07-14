"""Strategy: carrega dim_municipio (SCD Type 1)."""

from __future__ import annotations

from pyspark.sql import functions as F

from pipelines.batch.gold.base import DimLoader


class MunicipioDimLoader(DimLoader):
    def load(self, snapshot_date: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/municipio/")
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
        return self._write(df, "gold_dw.dim_municipio")
