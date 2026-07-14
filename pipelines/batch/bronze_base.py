"""Template Method for Bronze jobs.

Fixed flow (invariant):  read JSON → add metadata → add partition → validate → write Delta
What varies per source:  schema, source_name, partition_col, add_partition()

Subclasses only need to declare schema + partition_col and optionally
override add_partition() when the partition is derived from an existing field
(e.g.: vacinacao_pni derives year_month from data_vacina).
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class BronzeJob(ABC):
    """Template Method: landing JSON → Delta Lake (bronze/)."""

    # ── abstract hooks ───────────────────────────────────────────────────────

    @property
    @abstractmethod
    def schema(self) -> StructType:
        """Spark schema for reading raw JSON (StringType for all fields)."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Source name for logs and traceability."""

    @property
    @abstractmethod
    def partition_col(self) -> str:
        """Name of the Delta partition column (e.g.: 'year_month', 'snapshot_date')."""

    def add_partition(self, df: DataFrame, partition_val: str) -> DataFrame:
        """Hook: adds partition column to the DataFrame.

        Override when the partition is derived from an existing field
        (e.g.: vacinacao_pni derives year_month from data_vacina).
        Default: adds partition_col as a literal.
        """
        return df.withColumn(self.partition_col, F.lit(partition_val))

    # ── template method ──────────────────────────────────────────────────────

    def run(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        partition_val: str,
        batch_id: str,
        source_url: str,
    ) -> int:
        """Runs the full Bronze pipeline. Returns the number of rows written."""
        df = self._read(spark, input_path)
        df = self._add_metadata(df, source_url, batch_id)
        df = self.add_partition(df, partition_val)

        count = df.count()
        self._validate(count, input_path)
        log.info("[%s] %d rows | %s=%s", self.source_name, count, self.partition_col, partition_val)

        self._write(df, output_path, partition_val)
        return count

    # ── shared steps (do not override) ───────────────────────────────────────

    def _read(self, spark: SparkSession, input_path: str) -> DataFrame:
        return (
            spark.read
            .option("multiLine", "false")
            .schema(self.schema)
            .json(input_path)
        )

    def _add_metadata(self, df: DataFrame, source_url: str, batch_id: str) -> DataFrame:
        return (
            df
            .withColumn("_source_url", F.lit(source_url))
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
        )

    def _validate(self, count: int, input_path: str) -> None:
        if count == 0:
            raise ValueError(f"[{self.source_name}] No rows read from {input_path}")

    def _write(self, df: DataFrame, output_path: str, partition_val: str) -> None:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"{self.partition_col} = '{partition_val}'")
            .option("mergeSchema", "true")
            .partitionBy(self.partition_col)
            .save(output_path)
        )
