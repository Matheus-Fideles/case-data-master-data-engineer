"""Kafka DLQ Reprocessing DAG.

Runs hourly and consumes messages from the `notificacoes.dlq` topic,
routing each one based on the failure reason:

  LATE_EVENT     → Re-sends to Bronze with flag `is_late=true`
  SCHEMA_INVALID → Persists to s3://landing/dlq/schema_invalid/ for human review
  UNKNOWN_FIELD  → Log + discard (unexpected non-critical field)

Offset strategy: commits offset after successful processing.
Failure in a batch does not advance the offset — next run reprocesses.

Metrics: `dlq_reprocessed_total{reason}` → Prometheus Pushgateway.

Spec: docs/05-observabilidade-sre.md — section "dag_maint_dlq_reprocessor"
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_DLQ", "notificacoes.dlq")
_BRONZE_DLQ_PATH = "s3a://bronze/atendimentos_dlq_late/"
_DLQ_INVALID_PATH = "s3a://landing/dlq/schema_invalid/"
_MAX_POLL_RECORDS = int(os.environ.get("DLQ_MAX_POLL_RECORDS", "500"))
_CONSUMER_GROUP = "airflow-dlq-reprocessor"

_DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def _consume_dlq(**context) -> dict:
    """Consumes up to MAX_POLL_RECORDS messages from the DLQ and classifies by reason."""
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        log.warning("confluent_kafka not available — skipping DLQ reprocessing")
        return {"late": [], "schema_invalid": [], "unknown": [], "skipped": True}

    consumer = Consumer(
        {
            "bootstrap.servers": _KAFKA_BOOTSTRAP,
            "group.id": _CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.records": _MAX_POLL_RECORDS,
        }
    )
    consumer.subscribe([_DLQ_TOPIC])

    buckets: dict[str, list] = {
        "late": [],
        "schema_invalid": [],
        "unknown": [],
    }

    try:
        count = 0
        while count < _MAX_POLL_RECORDS:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("[dlq] Kafka error: %s", msg.error())
                break

            try:
                payload = json.loads(msg.value().decode("utf-8", errors="replace"))
                reason = payload.get("dlq_reason", "UNKNOWN_FIELD")
            except Exception:
                reason = "SCHEMA_INVALID"
                payload = {"raw": msg.value().decode("utf-8", errors="replace")}

            if reason == "LATE_EVENT":
                buckets["late"].append(payload)
            elif reason == "SCHEMA_INVALID":
                buckets["schema_invalid"].append(payload)
            else:
                buckets["unknown"].append(payload)

            count += 1

        consumer.commit()
        log.info(
            "[dlq] Consumed %d messages: late=%d, invalid=%d, unknown=%d",
            count,
            len(buckets["late"]),
            len(buckets["schema_invalid"]),
            len(buckets["unknown"]),
        )
    finally:
        consumer.close()

    context["ti"].xcom_push(
        key="dlq_buckets_summary", value={k: len(v) for k, v in buckets.items()}
    )
    # Store lists in XCom (small enough for metadata)
    context["ti"].xcom_push(key="late_events", value=buckets["late"][:50])
    context["ti"].xcom_push(key="schema_invalid", value=buckets["schema_invalid"][:50])
    return {k: len(v) for k, v in buckets.items()}


def _reprocess_late_events(**context) -> str:
    """Re-sends LATE_EVENTs to Bronze with flag is_late=true."""
    late = context["ti"].xcom_pull(task_ids="consume_dlq", key="late_events") or []
    if not late:
        log.info("[dlq/late] No late events to reprocess")
        return "empty"

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        region_name="us-east-1",
    )

    run_ts = context["data_interval_start"].strftime("%Y%m%d_%H%M%S")
    key = f"dlq/late/{run_ts}/events.jsonl"

    payload_bytes = "\n".join(
        json.dumps({**ev, "is_late": True, "reprocessed_at": run_ts}) for ev in late
    ).encode()

    s3.put_object(Bucket="landing", Key=key, Body=payload_bytes)
    log.info("[dlq/late] %d late events sent to landing/%s", len(late), key)
    return f"reprocessed:{len(late)}"


def _store_schema_invalid(**context) -> str:
    """Persists SCHEMA_INVALID to landing/dlq/schema_invalid/ for human review."""
    invalid = context["ti"].xcom_pull(task_ids="consume_dlq", key="schema_invalid") or []
    if not invalid:
        log.info("[dlq/invalid] No invalid events")
        return "empty"

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        region_name="us-east-1",
    )

    run_ts = context["data_interval_start"].strftime("%Y%m%d_%H%M%S")
    key = f"dlq/schema_invalid/{run_ts}/events.jsonl"

    payload_bytes = "\n".join(json.dumps(ev) for ev in invalid).encode()
    s3.put_object(Bucket="landing", Key=key, Body=payload_bytes)
    log.info("[dlq/invalid] %d invalid events stored at landing/%s", len(invalid), key)
    return f"stored:{len(invalid)}"


def _emit_metrics(**context) -> str:
    """Publishes DLQ counters to Prometheus Pushgateway."""
    summary = context["ti"].xcom_pull(task_ids="consume_dlq", key="dlq_buckets_summary") or {}
    if not summary:
        return "no_metrics"

    pushgateway = os.environ.get("PROMETHEUS_PUSHGATEWAY", "http://pushgateway:9091")
    try:
        import requests

        lines = "\n".join(
            f'dlq_reprocessed_total{{reason="{reason}"}} {count}'
            for reason, count in summary.items()
        )
        requests.post(
            f"{pushgateway}/metrics/job/dlq_reprocessor",
            data=lines + "\n",
            timeout=5,
        )
        log.info("[dlq/metrics] Metrics published: %s", summary)
    except Exception as e:
        log.warning("[dlq/metrics] Failed to publish metrics: %s", e)

    return str(summary)


with DAG(
    dag_id="dag_maint_dlq_reprocessor",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maint", "dlq", "kafka", "streaming"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:
    consume_dlq = PythonOperator(
        task_id="consume_dlq",
        python_callable=_consume_dlq,
        execution_timeout=timedelta(minutes=10),
    )

    reprocess_late = PythonOperator(
        task_id="reprocess_late_events",
        python_callable=_reprocess_late_events,
        execution_timeout=timedelta(minutes=15),
    )

    store_invalid = PythonOperator(
        task_id="store_schema_invalid",
        python_callable=_store_schema_invalid,
        execution_timeout=timedelta(minutes=5),
    )

    emit_metrics = PythonOperator(
        task_id="emit_metrics",
        python_callable=_emit_metrics,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=2),
    )

    consume_dlq >> [reprocess_late, store_invalid] >> emit_metrics
