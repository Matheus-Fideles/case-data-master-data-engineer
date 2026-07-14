"""Strategy: carrega dim_tempo (gerada programaticamente para o mês)."""
from __future__ import annotations

import calendar

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

from pipelines.batch.gold.base import DimLoader


class TempoDimLoader(DimLoader):

    def load(self, ano_mes: str, **_) -> int:
        ano = int(ano_mes[:4])
        mes = int(ano_mes[4:6])
        dias_no_mes = calendar.monthrange(ano, mes)[1]
        start = f"{ano}-{mes:02d}-01"

        df = (
            self._spark.range(0, dias_no_mes)
            .withColumn(
                "data",
                F.date_add(F.lit(start).cast(DateType()), F.col("id").cast(IntegerType())),
            )
            .withColumn("ano",                 F.year("data"))
            .withColumn("mes",                 F.month("data"))
            .withColumn("dia",                 F.dayofmonth("data"))
            .withColumn("semana_epidemiologica", F.weekofyear("data"))
            .withColumn("trimestre",           F.quarter("data"))
            .withColumn("dia_semana",          F.dayofweek("data"))
            .drop("id")
        )
        return self._write(df, "gold_dw.dim_tempo", mode="append")
