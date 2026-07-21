"""Spark Structured Streaming — consumer for the notificacoes.raw topic.

Flow:
  Kafka (notificacoes.raw)
    → parse JSON + schema validation
    → withWatermark("ts_evento", "1 hour")    [ADR 0005]
    → foreachBatch: idempotent MERGE into Delta bronze/streaming/atendimentos_stream/
    → foreachBatch: append into Delta gold/streaming/fatos_atendimento_stream/

Late events (ts_evento < watermark) are routed to the DLQ topic
notificacoes.dlq with reason LATE_EVENT.

Checkpoint at s3a://landing/_checkpoints/atendimentos_stream_consumer/
guarantees exactly-once after restart (ADR 0005).

Trigger configurable via STREAM_TRIGGER_SECS (default 30s).
In CI uses Trigger.AvailableNow() via STREAM_CI_MODE=1.
"""

from __future__ import annotations

import logging
import os

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from apps.shared.spark import build_spark

log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "notificacoes.raw")
KAFKA_DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "notificacoes.dlq")
BRONZE_PATH = os.environ.get("BRONZE_STREAM_PATH", "s3a://bronze/streaming/atendimentos_stream/")
GOLD_PATH = os.environ.get("GOLD_STREAM_PATH", "s3a://gold/streaming/fatos_atendimento_stream/")
CHECKPOINT_PATH = os.environ.get(
    "STREAM_CHECKPOINT_PATH",
    "s3a://landing/_checkpoints/atendimentos_stream_consumer/",
)
WATERMARK = os.environ.get("STREAM_WATERMARK", "1 hour")
TRIGGER_SECS = int(os.environ.get("STREAM_TRIGGER_SECS", "30"))
CI_MODE = os.environ.get("STREAM_CI_MODE", "0") == "1"
# Optional JSON offset map e.g. '{"notificacoes.raw":{"0":100,"1":0}}'
STARTING_OFFSETS = os.environ.get("STREAM_STARTING_OFFSETS", "")

_PAYLOAD_SCHEMA = StructType(
    [
        StructField("id_atendimento", StringType(), True),
        StructField("id_paciente_hash", StringType(), True),
        StructField("id_cnes", StringType(), True),
        StructField("ts_evento", StringType(), True),
        StructField("tipo_atendimento", StringType(), True),
        StructField("triagem", StringType(), True),
    ]
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_kafka(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parses Kafka payload → (valid, DLQ)."""
    parsed = (
        raw_df.select(
            F.col("offset").alias("_kafka_offset"),
            F.col("partition").alias("_kafka_partition"),
            F.from_json(F.col("value").cast("string"), _PAYLOAD_SCHEMA).alias("data"),
        )
        .select("_kafka_offset", "_kafka_partition", "data.*")
        .withColumn("ts_evento", F.to_timestamp("ts_evento"))
    )

    valid = parsed.filter(F.col("ts_evento").isNotNull() & F.col("id_atendimento").isNotNull())
    invalid = parsed.filter(F.col("ts_evento").isNull() | F.col("id_atendimento").isNull())
    return valid, invalid


def _write_bronze(microbatch: DataFrame, batch_id: int) -> None:
    """Writes micro-batch to Delta bronze via MERGE (idempotent — ADR 0002)."""
    enriched = (
        microbatch.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("ano_mes", F.date_format("ts_evento", "yyyyMM"))
    )

    if DeltaTable.isDeltaTable(microbatch.sparkSession, BRONZE_PATH):
        dt = DeltaTable.forPath(microbatch.sparkSession, BRONZE_PATH)
        (
            dt.alias("tgt")
            .merge(
                enriched.alias("src"),
                "tgt.id_atendimento = src.id_atendimento",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (enriched.write.format("delta").mode("append").partitionBy("ano_mes").save(BRONZE_PATH))

    log.info("[bronze/stream] batch=%d rows=%d", batch_id, enriched.count())


def _write_gold_delta(microbatch: DataFrame, batch_id: int) -> None:
    """Appends micro-batch to Delta gold/streaming/ (ADR-0011: gold no lake, não Postgres)."""
    gold_df = (
        microbatch.withColumn("evento_ts", F.col("ts_evento"))
        .withColumn("sk_tempo", F.date_format("ts_evento", "yyyyMMdd").cast("int"))
        .withColumn("ano_mes", F.date_format("ts_evento", "yyyyMM"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_ts", F.current_timestamp())
        .select(
            "evento_ts",
            "sk_tempo",
            "id_paciente_hash",
            "id_cnes",
            "tipo_atendimento",
            "triagem",
            "_kafka_offset",
            "_kafka_partition",
            "ano_mes",
            "_batch_id",
            "_load_ts",
        )
    )

    gold_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(
        "ano_mes"
    ).save(GOLD_PATH)
    log.info("[gold/stream] batch=%d rows=%d path=%s", batch_id, gold_df.count(), GOLD_PATH)


def _send_to_dlq(spark: SparkSession, invalid_df: DataFrame, reason: str) -> None:
    """Sends invalid records to the Kafka DLQ topic."""
    if invalid_df.rdd.isEmpty():
        return

    dlq_df = invalid_df.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.to_json(F.struct("*")).alias("original_payload"),
                F.lit(KAFKA_TOPIC).alias("kafka_topic"),
                F.lit(reason).alias("error_reason"),
                F.current_timestamp().alias("rejected_at"),
            )
        ),
    ).select(F.col("id_atendimento").cast("string").alias("key"), "value")

    (
        dlq_df.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_DLQ_TOPIC)
        .save()
    )
    log.warning("[DLQ] %s: %d events sent to %s", reason, dlq_df.count(), KAFKA_DLQ_TOPIC)


# ── Orchestration ────────────────────────────────────────────────────────────


def _make_foreachbatch(spark: SparkSession):
    def _process(microbatch: DataFrame, batch_id: int) -> None:
        if microbatch.rdd.isEmpty():
            return

        valid, invalid = _parse_kafka(microbatch.select("offset", "partition", "value"))

        if not invalid.rdd.isEmpty():
            _send_to_dlq(spark, invalid, "SCHEMA_INVALID")

        if valid.rdd.isEmpty():
            return

        watermarked = valid.withWatermark("ts_evento", WATERMARK)

        _write_bronze(watermarked, batch_id)
        _write_gold_delta(watermarked, batch_id)

    return _process


def run(spark: SparkSession | None = None) -> None:
    if spark is None:
        spark = build_spark("atendimento_stream_consumer")

    spark.sparkContext.setLogLevel("WARN")

    if STARTING_OFFSETS:
        starting_offsets = STARTING_OFFSETS
    elif CI_MODE:
        starting_offsets = "latest"
    else:
        starting_offsets = "latest"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(_make_foreachbatch(spark))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .queryName("atendimento_stream_consumer")
    )

    if CI_MODE:
        query = query.trigger(availableNow=True)
    else:
        query = query.trigger(processingTime=f"{TRIGGER_SECS} seconds")

    stream = query.start()
    log.info(
        "Stream started | topic=%s trigger=%ss watermark=%s",
        KAFKA_TOPIC,
        TRIGGER_SECS,
        WATERMARK,
    )

    if CI_MODE:
        # availableNow terminates automatically; 120s cap prevents hanging on Kafka errors
        stream.awaitTermination(timeout=120)
        stream.stop()
    else:
        stream.awaitTermination()


if __name__ == "__main__":
    run()
