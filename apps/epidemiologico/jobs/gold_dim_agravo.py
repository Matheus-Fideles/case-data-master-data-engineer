"""Strategy: loads dim_agravo (static seed table)."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from apps.shared.gold_base import DimLoader

_SEED = [
    ("A90", "Dengue", "Arbovirose", "SINAN"),
    ("A92.0", "Zika", "Arbovirose", "SINAN"),
    ("A92.3", "Chikungunya", "Arbovirose", "SINAN"),
]

_SCHEMA = StructType(
    [
        StructField("id_agravo", StringType(), False),
        StructField("nome_agravo", StringType(), False),
        StructField("categoria", StringType(), True),
        StructField("sistema", StringType(), True),
    ]
)


class AgravoDimLoader(DimLoader):
    def load(self, **_) -> int:
        df = self._spark.createDataFrame(_SEED, _SCHEMA)
        return self._write(df, "s3a://gold/epidemiologico/dim_agravo/")
