"""Template Method para jobs Bronze.

Fluxo fixo (invariante):  read JSON → add metadata → add partition → validate → write Delta
O que varia por fonte:     schema, source_name, partition_col, add_partition()

Subclasses só precisam declarar schema + partition_col e opcionalmente
sobrescrever add_partition() quando a partição é derivada de um campo
(ex.: vacinacao_pni deriva ano_mes de data_vacina).
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
        """Schema Spark para leitura do JSON bruto (StringType para todos os campos)."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Nome da fonte para logs e rastreabilidade."""

    @property
    @abstractmethod
    def partition_col(self) -> str:
        """Nome da coluna de partição Delta (ex.: 'ano_mes', 'snapshot_date')."""

    def add_partition(self, df: DataFrame, partition_val: str) -> DataFrame:
        """Hook: adiciona coluna de partição ao DataFrame.

        Override quando a partição é derivada de um campo existente
        (ex.: vacinacao_pni deriva ano_mes de data_vacina).
        Padrão: adiciona partition_col como literal.
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
        """Executa o pipeline Bronze completo. Retorna o número de linhas escritas."""
        df = self._read(spark, input_path)
        df = self._add_metadata(df, source_url, batch_id)
        df = self.add_partition(df, partition_val)

        count = df.count()
        self._validate(count, input_path)
        log.info("[%s] %d linhas | %s=%s", self.source_name, count, self.partition_col, partition_val)

        self._write(df, output_path, partition_val)
        return count

    # ── passos comuns (não sobrescrever) ─────────────────────────────────────

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
            raise ValueError(f"[{self.source_name}] Nenhuma linha lida de {input_path}")

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
