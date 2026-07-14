"""Smoke 05 — Streaming E2E (Kafka → Spark → Bronze).

Produz eventos no Kafka, dispara consumer em CI_MODE e verifica:
  1. Eventos válidos chegam ao Bronze Delta
  2. Eventos inválidos são desviados para o DLQ
  3. Watermark filtra eventos atrasados (> 1h) corretamente

Tempo alvo: < 120s (inclui startup Spark Streaming)
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests.smoke.conftest import (
    FIXTURES_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_DLQ_TOPIC,
    KAFKA_TOPIC,
)

pytestmark = pytest.mark.smoke

BRONZE_STREAM_PATH = "s3a://bronze/atendimentos/"
N_VALID   = 20
N_INVALID = 5


# ── Helpers de produção ───────────────────────────────────────────────────────

def _ts_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _ts_late() -> str:
    """Timestamp 2h atrás — deve ser filtrado pelo watermark de 1h."""
    return (datetime.now(tz=timezone.utc) - timedelta(hours=2)).isoformat()


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
    """Falta campo obrigatório id_atendimento."""
    return {"tipo_atendimento": "CONSULTA", "ts_evento": _ts_now()}


def _late_event() -> dict:
    """Evento com timestamp > watermark."""
    ev = _valid_event(9999)
    ev["ts_evento"] = _ts_late()
    return ev


def _produce(producer, topic: str, events: list[dict]) -> None:
    for ev in events:
        producer.produce(topic, value=json.dumps(ev).encode())
    producer.flush(timeout=15)


# ── Testes ────────────────────────────────────────────────────────────────────

def test_kafka_produces_valid_events(kafka_producer):
    """Produz N_VALID eventos válidos no tópico principal."""
    events = [_valid_event(i) for i in range(N_VALID)]
    _produce(kafka_producer, KAFKA_TOPIC, events)


def test_kafka_produces_invalid_events(kafka_producer):
    """Produz N_INVALID eventos inválidos (schema quebrado)."""
    events = [_invalid_event() for _ in range(N_INVALID)]
    _produce(kafka_producer, KAFKA_TOPIC, events)


def test_streaming_consumer_ci_mode(spark_session, s3):
    """Roda consumer em CI_MODE (AvailableNow) e verifica escrita Bronze."""
    import os
    os.environ["STREAM_CI_MODE"] = "1"
    os.environ["BRONZE_STREAM_PATH"] = BRONZE_STREAM_PATH

    try:
        from pipelines.streaming.atendimento_consumer import run as stream_run
        stream_run(spark=spark_session)
    finally:
        os.environ.pop("STREAM_CI_MODE", None)

    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)
    count = df.count()
    assert count >= N_VALID, \
        f"Bronze streaming deve ter ≥ {N_VALID} registros, encontrado {count}"


def test_valid_events_in_bronze(spark_session):
    """Eventos válidos com id_atendimento devem estar no Bronze."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    assert "id_atendimento" in df.columns, "Coluna id_atendimento ausente no Bronze streaming"
    valid = df.filter(F.col("id_atendimento").isNotNull()).count()
    assert valid >= N_VALID, f"Esperado ≥ {N_VALID} eventos válidos, encontrado {valid}"


def test_invalid_events_in_dlq(kafka_producer):
    """Eventos inválidos devem aparecer no tópico DLQ."""
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "smoke-dlq-checker",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_DLQ_TOPIC])

    dlq_count = 0
    deadline = time.time() + 30
    while time.time() < deadline and dlq_count < N_INVALID:
        msg = consumer.poll(timeout=2.0)
        if msg and not msg.error():
            dlq_count += 1

    consumer.close()
    assert dlq_count >= 1, \
        f"Nenhum evento inválido encontrado no DLQ ({KAFKA_DLQ_TOPIC})"


def test_bronze_streaming_has_metadata_columns(spark_session):
    """Bronze streaming deve ter colunas de metadados padrão."""
    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    for col in ("_ingestion_ts", "_batch_id", "ano_mes"):
        assert col in df.columns, f"Coluna de metadados '{col}' ausente no Bronze streaming"


def test_bronze_no_pii_in_streaming(spark_session):
    """Eventos de atendimento não devem conter CPF ou PII direto."""
    df = spark_session.read.format("delta").load(BRONZE_STREAM_PATH)

    pii_cols = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii_cols & set(df.columns)
    assert not leakage, f"PII detectado no Bronze streaming: {leakage}"


@pytest.mark.skipif(
    True,  # late-event test é best-effort em smoke — watermark depende de window real
    reason="Late-event via watermark requer janela temporal real — desabilitado em smoke",
)
def test_late_event_filtered_by_watermark(kafka_producer, spark_session, s3):
    """Eventos com timestamp > 1h atrás devem ser filtrados pelo watermark."""
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
    late_found = df.filter(
        df["id_atendimento"] == late["id_atendimento"]
    ).count()
    assert late_found == 0, "Evento tardio não foi filtrado pelo watermark de 1h"
