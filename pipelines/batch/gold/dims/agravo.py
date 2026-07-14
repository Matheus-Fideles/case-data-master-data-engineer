"""Strategy: carrega dim_agravo (tabela estática — seed)."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from pipelines.batch.gold.base import DimLoader

_SEED = [
    ("A90",   "Dengue",       "Arbovirose", "SINAN"),
    ("A92.0", "Zika",         "Arbovirose", "SINAN"),
    ("A92.3", "Chikungunya",  "Arbovirose", "SINAN"),
]

_SCHEMA = StructType([
    StructField("id_agravo",   StringType(), False),
    StructField("nome_agravo", StringType(), False),
    StructField("categoria",   StringType(), True),
    StructField("sistema",     StringType(), True),
])


class AgravoDimLoader(DimLoader):

    def load(self, **_) -> int:
        df = self._spark.createDataFrame(_SEED, _SCHEMA)
        return self._write(df, "gold_dw.dim_agravo")
