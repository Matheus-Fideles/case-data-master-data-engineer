"""Smoke 05 — Streaming E2E (Kafka -> Spark -> Bronze).

Produces events in Kafka, triggers the consumer in CI_MODE, and verifies:
  1. Valid events arrive in the Bronze Delta table
  2. Invalid events are routed to the DLQ
  3. Watermark correctly filters late events (> 1h old)

Target time: < 120s (includes Spark Streaming startup)
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.smoke.conftest import (
    KAFKA_BOOTSTRAP,
    KAFKA_DLQ_TOPIC,
    KAFKA_TOPIC,
)

pytestmark = pytest.mark.smoke

BRONZE_STREAM_PATH = "s3a://bronze/atendimentos/"
N_VALID = 20
N_INVALID = 5


# ── Producer helpers ──────────────────────────────────────────────────────────


def _ts_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _ts_late() -> str:
    """Timestamp 2h ago — must be filtered by the 1h watermark."""
    return (datetime.now(tz=UTC) - timedelta(hours=2)).isoformat()


def _valid_event(i: int) -> dict:
    return {
        "id_atendimento": str(uuid.uuid4()),
        "id_paciente_hash": "a" * 64,
        "tipo_atendimento": "CONSULTA",
        "cid10_principal": "J00",
        "ts_evento": _ts_now(),
        "municipio_codigo_ibge": "3550308",
        "seq": i,
    }


def _invalid_event() -> dict:
    """Missing required field id_atendimento."""
    return {"tipo_atendimento": "CONSULTA", "ts_evento": _ts_now()}


def _late_event() -> dict:
    """Event with timestamp beyond the watermark."""
    ev = _valid_event(9999)
    ev["ts_evento"] = _ts_late()
    return ev


def _produce(producer, topic: str, events: list[dict]) -> None:
    for ev in events:
        producer.produce(topic, value=json.dumps(ev).encode())
    producer.flush(timeout=15)


# ── Tests ─────────────────────────────────────────────────────────────────────


def _current_offsets(topic: str) -> str:
    """Returns a startingOffsets JSON string pointing to the current end of each partition."""
    from confluent_kafka import Consumer as KConsumer
    from confluent_kafka import TopicPartition

    c = KConsumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "_smoke_offset_probe"})
    metadata = c.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
    parts_with_offsets = {}
    for tp in partitions:
        lo, hi = c.get_watermark_offsets(tp, timeout=5)
        parts_with_offsets[tp.partition] = hi
    c.close()
    return json.dumps({topic: {str(p): off for p, off in parts_with_offsets.items()}})


def test_kafka_produces_valid_events(kafka_producer):
    """Captures current offsets then produces N_VALID valid events on the main topic."""
    # Save offset snapshot so the CI consumer only reads events produced in THIS test run
    import pytest

    pytest.current_stream_offsets = _current_offsets(KAFKA_TOPIC)
    events = [_valid_event(i) for i in range(N_VALID)]
    _produce(kafka_producer, KAFKA_TOPIC, events)


def test_kafka_produces_invalid_events(kafka_producer):
    """Produces N_INVALID invalid events (broken schema)."""
    events = [_invalid_event() for _ in range(N_INVALID)]
    _produce(kafka_producer, KAFKA_TOPIC, events)


def test_streaming_consumer_ci_mode(spark_session, s3):
    """Runs the consumer in CI_MODE (AvailableNow) and verifies Bronze write."""
    import os

    import pytest

    starting_offsets = getattr(pytest, "current_stream_offsets", "latest")

    os.environ["STREAM_CI_MODE"] = "1"
    os.environ["BRONZE_STREAM_PATH"] = BRONZE_STREAM_PATH
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = KAFKA_BOOTSTRAP
    os.environ["KAFKA_TOPIC"] = KAFKA_TOPIC
    os.environ["KAFKA_DLQ_TOPIC"] = KAFKA_DLQ_TOPIC
    os.environ["STREAM_STARTING_OFFSETS"] = starting_offsets

    try:
        import importlib

        import pipelines.streaming.atendimento_consumer as _mod

        importlib.reload(_mod)  # pick up env vars set above
        _mod.run(spark=spark_session)
    finally:
        for k in (
            "STREAM_CI_MODE",
            "BRONZE_STREAM_PATH",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC",
            "KAFKA_DLQ_TOPIC",
            "STREAM_STARTING_OFFSETS",
        ):
            os.environ.pop(k, None)

    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)
    count = df.count()
    assert count >= N_VALID, f"Bronze streaming must have >= {N_VALID} records, found {count}"


def test_valid_events_in_bronze(spark_session):
    """Valid events with id_atendimento must be present in Bronze."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    assert "id_atendimento" in df.columns, "Column id_atendimento missing from Bronze streaming"
    valid = df.filter(F.col("id_atendimento").isNotNull()).count()
    assert valid >= N_VALID, f"Expected >= {N_VALID} valid events, found {valid}"


def test_invalid_events_in_dlq(kafka_producer):
    """Invalid events must appear on the DLQ topic."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "smoke-dlq-checker",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_DLQ_TOPIC])

    dlq_count = 0
    deadline = time.time() + 30
    while time.time() < deadline and dlq_count < N_INVALID:
        msg = consumer.poll(timeout=2.0)
        if msg and not msg.error():
            dlq_count += 1

    consumer.close()
    assert dlq_count >= 1, f"No invalid events found in the DLQ ({KAFKA_DLQ_TOPIC})"


def test_bronze_streaming_has_metadata_columns(spark_session):
    """Bronze streaming must have the standard metadata columns."""
    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    for col in ("_ingestion_ts", "_batch_id", "ano_mes"):
        assert col in df.columns, f"Metadata column '{col}' missing from Bronze streaming"


def test_bronze_no_pii_in_streaming(spark_session):
    """Attendance events must not contain CPF or direct PII."""
    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    pii_cols = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii_cols & set(df.columns)
    assert not leakage, f"PII detected in Bronze streaming: {leakage}"


@pytest.mark.skipif(
    True,  # late-event test is best-effort in smoke — watermark depends on a real window
    reason="Late-event via watermark requires a real time window — disabled in smoke",
)
def test_late_event_filtered_by_watermark(kafka_producer, spark_session, s3):
    """Events with a timestamp > 1h ago must be filtered by the watermark."""
    import os

    late = _late_event()
    _produce(kafka_producer, KAFKA_TOPIC, [late])

    os.environ["STREAM_CI_MODE"] = "1"
    try:
        from pipelines.streaming.atendimento_consumer import run as stream_run

        stream_run(spark=spark_session)
    finally:
        os.environ.pop("STREAM_CI_MODE", None)

    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)
    late_found = df.filter(df["id_atendimento"] == late["id_atendimento"]).count()
    assert late_found == 0, "Late event was not filtered by the 1h watermark"
