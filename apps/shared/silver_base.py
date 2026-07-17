"""Template Method for Silver jobs.

Fixed flow:  read Delta (bronze) → filter → transform → add metadata → write Delta (silver)
What varies: type normalization, column names, PII masking — implemented in transform().

For composite partitions (e.g.: silver/notificacao/ uses disease + year_month),
the subclass can override replace_condition().
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)


class SilverJob(ABC):
    """Template Method: bronze Delta → silver Delta."""

    # ── abstract hooks ───────────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> str: ...

    @property
    @abstractmethod
    def partition_cols(self) -> list[str]:
        """Delta Silver partition columns (can be more than one)."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Normalizes types, renames columns, and applies PII masking."""

    def replace_condition(self, filter_col: str, filter_val: str) -> str:
        """replaceWhere condition. Override for composite partitions."""
        return f"{filter_col} = '{filter_val}'"

    # ── template method ──────────────────────────────────────────────────────

    def run(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        filter_col: str,
        filter_val: str,
        batch_id: str,
    ) -> int:
        df = self._read(spark, input_path, filter_col, filter_val)
        df = self.transform(df)
        df = self._add_metadata(df, batch_id)

        count = df.count()
        log.info("[%s] %d rows | %s=%s", self.source_name, count, filter_col, filter_val)

        self._write(df, output_path, filter_col, filter_val)
        return count

    # ── shared steps ─────────────────────────────────────────────────────────

    def _read(self, spark: SparkSession, path: str, filter_col: str, filter_val: str) -> DataFrame:
        return spark.read.format("delta").load(path).filter(F.col(filter_col) == filter_val)

    def _add_metadata(self, df: DataFrame, batch_id: str) -> DataFrame:
        return (
            df.withColumn("_silver_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
            .drop("_source_url", "_ingestion_ts")
        )

    def _write(self, df: DataFrame, output_path: str, filter_col: str, filter_val: str) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", self.replace_condition(filter_col, filter_val))
            .option("mergeSchema", "true")
            .partitionBy(*self.partition_cols)
            .save(output_path)
        )
