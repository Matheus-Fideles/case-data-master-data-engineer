"""Contracts (ABC) for Gold loaders — Strategy Pattern.

DimLoader and FatoLoader define the interface each Strategy implements.
The dispatcher in gold_dims.py and gold_fatos.py selects the Strategy by name.

Benefit: adding a new dimension = creating a new file in dims/,
without touching the dispatcher or other strategies (OCP).
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


class DimLoader(ABC):
    """Strategy: loads a dimension into the gold_dw schema."""

    def __init__(self, spark: SparkSession, pg_url: str, pg_props: dict) -> None:
        self._spark = spark
        self._pg_url = pg_url
        self._pg_props = pg_props

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Runs the load. Returns the number of rows written."""

    def _write(self, df: DataFrame, table: str, mode: str = "overwrite") -> int:
        count = df.count()
        df.write.jdbc(self._pg_url, table, mode=mode, properties=self._pg_props)
        log.info("[gold/%s] %d rows written (mode=%s)", table, count, mode)
        return count


class FatoLoader(ABC):
    """Strategy: loads a fact into the gold_dw schema."""

    def __init__(self, spark: SparkSession, pg_url: str, pg_props: dict) -> None:
        self._spark = spark
        self._pg_url = pg_url
        self._pg_props = pg_props

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Runs the load. Returns the number of rows written."""

    def _write(self, df: DataFrame, table: str) -> int:
        count = df.count()
        df.write.jdbc(self._pg_url, table, mode="append", properties=self._pg_props)
        log.info("[gold/%s] %d rows written", table, count)
        return count
