"""Strategy: carrega fato_obito (SIM)."""

from __future__ import annotations

from pyspark.sql import functions as F

from pipelines.batch.gold.base import FatoLoader


class ObitoFatoLoader(FatoLoader):
    def load(self, ano: str, batch_id: str, **_) -> int:
        df = (
            self._spark.read.format("delta")
            .load("s3a://silver/sim_obitos/")
            .filter(F.col("ano_part") == ano)
            .select("dtobito", "codmunocor", "causabas", "idade", "sexo", "ano_part")
            .withColumnRenamed("dtobito", "dt_obito")
            .withColumnRenamed("codmunocor", "co_municipio_ocor")
            .withColumnRenamed("causabas", "id_causa_basica")
            .withColumn("co_municipio_ocor", F.col("co_municipio_ocor").cast("int"))
            .withColumn("sk_tempo", F.date_format(F.col("dt_obito"), "yyyyMMdd").cast("int"))
            .withColumn("qtd_obitos", F.lit(1))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_load_ts", F.current_timestamp())
        )
        return self._write(df, "gold_dw.fato_obito")
