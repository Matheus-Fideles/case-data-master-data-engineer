"""Contracts (ABC) for Gold loaders — Strategy Pattern.

DimLoader and FatoLoader define the interface each Strategy implements.
The dispatcher in gold_dims.py and gold_fatos.py selects the Strategy by name.

Benefit: adding a new dimension = creating a new file in the domain jobs/,
without touching the dispatcher or other strategies (OCP).

Gold writes to Delta Lake on MinIO (not Postgres).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


class DimLoader(ABC):
    """Strategy: loads a dimension into the Gold Delta Lake."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Runs the load. Returns the number of rows written."""

    def _write(self, df: DataFrame, output_path: str, mode: str = "overwrite") -> int:
        count = df.count()
        (df.write.format("delta").mode(mode).option("mergeSchema", "true").save(output_path))
        log.info("[gold/%s] %d rows written (mode=%s)", output_path.split("/")[-2], count, mode)
        return count


class FatoLoader(ABC):
    """Strategy: loads a fact into the Gold Delta Lake."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    @abstractmethod
    def load(self, **kwargs) -> int:
        """Runs the load. Returns the number of rows written."""

    def _write(self, df: DataFrame, output_path: str) -> int:
        count = df.count()
        (df.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path))
        log.info("[gold/%s] %d rows written", output_path.split("/")[-2], count)
        return count
