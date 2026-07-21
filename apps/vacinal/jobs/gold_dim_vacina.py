"""Strategy: carrega dim_vacina (SCD Type 1)."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from apps.shared.gold_base import DimLoader

log = logging.getLogger(__name__)

_SILVER_PATH = "s3a://silver/vacinal/vacinacao_pni/"


class VacinaDimLoader(DimLoader):
    def load(self, ano_mes: str, **_) -> int:
        try:
            df = (
                self._spark.read.format("delta")
                .load(_SILVER_PATH)
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
        except Exception as exc:  # noqa: BLE001
            log.warning("dim_vacina: silver data not available (%s) — skipping", exc)
            return 0
