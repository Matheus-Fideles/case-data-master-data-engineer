"""Smoke 01 — Infrastructure health (Docker Compose + dependencies).

Verifies that all required services are responding before any pipeline
test runs. A failure here stops the entire suite (xfail_strict).

Target time: < 30s
"""
from __future__ import annotations

import os

import pytest
import requests

from tests.smoke.conftest import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MINIO_BUCKETS,
    MINIO_ENDPOINT,
    PG_HOST,
    PG_PORT,
    pg_conn,
    minio_client,
)

pytestmark = pytest.mark.smoke


# ── Postgres ──────────────────────────────────────────────────────────────────

def test_postgres_accepts_connection(compose_up):
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    conn.close()


def test_oltp_schema_exists(compose_up):
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'oltp'")
    assert cur.fetchone() is not None, "Schema 'oltp' does not exist"
    conn.close()


def test_gold_dw_schema_exists(compose_up):
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold_dw'")
    assert cur.fetchone() is not None, "Schema 'gold_dw' does not exist"
    conn.close()


# ── MinIO ─────────────────────────────────────────────────────────────────────

def test_minio_health_endpoint(compose_up):
    resp = requests.head(f"{MINIO_ENDPOINT}/minio/health/live", timeout=10)
    assert resp.status_code == 200, f"MinIO health returned {resp.status_code}"


def test_minio_required_buckets_exist(compose_up):
    s3 = minio_client()
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    missing = MINIO_BUCKETS - existing
    assert not missing, f"Buckets missing from MinIO: {missing}"


def test_minio_buckets_are_not_public(compose_up):
    """Ensures no bucket has anonymous access enabled."""
    s3 = minio_client()
    for bucket in MINIO_BUCKETS:
        try:
            policy = s3.get_bucket_policy(Bucket=bucket)
            # If we get here, there is a policy — verify it is not AllUsers
            assert "AllUsers" not in str(policy.get("Policy", "")), \
                f"Bucket '{bucket}' has a public policy (AllUsers)"
        except s3.exceptions.from_code("NoSuchBucketPolicy"):
            pass  # no policy = default private access ✓
        except Exception as e:
            # NoSuchBucketPolicy raised as ClientError in boto3
            if "NoSuchBucketPolicy" in str(e):
                pass
            else:
                raise


# ── Kafka ─────────────────────────────────────────────────────────────────────

def test_kafka_accepts_connection(compose_up):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(timeout=10)
    assert metadata is not None


def test_kafka_topic_notificacoes_exists_or_creatable(compose_up):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = admin.list_topics(timeout=10).topics

    if KAFKA_TOPIC not in topics:
        fs = admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=1)])
        for topic, f in fs.items():
            exc = f.exception()
            assert exc is None, f"Failed to create topic {topic}: {exc}"


# ── Airflow ───────────────────────────────────────────────────────────────────

@pytest.mark.skipif(
    os.environ.get("SKIP_AIRFLOW_CHECK", "0") == "1",
    reason="Airflow is not in the core profile",
)
def test_airflow_health_endpoint(compose_up):
    resp = requests.get("http://localhost:8080/health", timeout=10)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "healthy", f"Airflow not healthy: {data}"
