"""Strategy: carrega fato_vacinacao (PNI)."""

from __future__ import annotations

from pyspark.sql import functions as F

from pipelines.batch.gold.base import FatoLoader


class VacinacaoFatoLoader(FatoLoader):
    def load(self, ano_mes: str, batch_id: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/vacinacao_pni/")
            .filter(F.col("ano_mes") == ano_mes)
            .select(
                "data_vacina",
                "codigo_municipio_paciente",
                "codigo_vacina",
                "codigo_cnes_estabelecimento",
                "codigo_paciente",
                "ano_mes",
            )
            .withColumn(
                "sk_tempo",
                F.date_format(F.col("data_vacina"), "yyyyMMdd").cast("int"),
            )
            .withColumn("qtd_doses", F.lit(1))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_load_ts", F.current_timestamp())
        )
        return self._write(df, "gold_dw.fato_vacinacao")
