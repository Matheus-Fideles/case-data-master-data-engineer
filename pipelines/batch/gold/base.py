"""Contratos (ABC) para loaders Gold — Strategy Pattern.

DimLoader e FatoLoader definem a interface que cada Strategy implementa.
O dispatcher em gold_dims.py e gold_fatos.py seleciona a Strategy pelo nome.

Benefício: adicionar uma nova dimensão = criar novo arquivo em dims/,
sem tocar no dispatcher nem nas demais strategies (OCP).
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


class DimLoader(ABC):
    """Strategy: carrega uma dimensão no schema gold_dw."""

    def __init__(self, spark: SparkSession, pg_url: str, pg_props: dict) -> None:
        self._spark = spark
        self._pg_url = pg_url
        self._pg_props = pg_props

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Executa a carga. Retorna número de linhas escritas."""

    def _write(self, df: DataFrame, table: str, mode: str = "overwrite") -> int:
        count = df.count()
        df.write.jdbc(self._pg_url, table, mode=mode, properties=self._pg_props)
        log.info("[gold/%s] %d linhas escritas (mode=%s)", table, count, mode)
        return count


class FatoLoader(ABC):
    """Strategy: carrega um fato no schema gold_dw."""

    def __init__(self, spark: SparkSession, pg_url: str, pg_props: dict) -> None:
        self._spark = spark
        self._pg_url = pg_url
        self._pg_props = pg_props

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Executa a carga. Retorna número de linhas escritas."""

    def _write(self, df: DataFrame, table: str) -> int:
        count = df.count()
        df.write.jdbc(self._pg_url, table, mode="append", properties=self._pg_props)
        log.info("[gold/%s] %d linhas escritas", table, count)
        return count
