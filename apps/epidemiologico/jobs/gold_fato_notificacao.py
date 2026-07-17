"""Strategy: carrega fato_notificacao."""

from __future__ import annotations

from pyspark.sql import functions as F

from apps.shared.gold_base import FatoLoader


class NotificacaoFatoLoader(FatoLoader):
    def load(self, ano_mes: str, batch_id: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/epidemiologico/notificacao/")
            .filter(F.col("ano_mes") == ano_mes)
            .select(
                "id_agravo",
                "dt_notific",
                "sg_uf_not",
                "id_municip",
                "nu_idade_n",
                "cs_sexo",
                "classi_fin",
                "evolucao",
                "hospitaliz",
                "agravo",
                "ano_mes",
            )
            .withColumn("co_municipio", F.col("id_municip").cast("int"))
            .withColumn("sk_tempo", F.date_format(F.col("dt_notific"), "yyyyMMdd").cast("int"))
            .withColumn("qtd_casos", F.lit(1))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_load_ts", F.current_timestamp())
            .drop("id_municip")
        )
        return self._write(df, "s3a://gold/epidemiologico/fatos_notificacao/")
