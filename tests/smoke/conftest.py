"""Shared fixtures for smoke tests.

All fixtures require real infrastructure running (docker compose up).
To run locally without tearing down compose: KEEP_COMPOSE=1 pytest tests/smoke/
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Generator

import pytest

# ── Connection constants ──────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT_EXTERNAL", "http://localhost:9000")
MINIO_ACCESS   = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKETS  = {"landing", "bronze", "silver", "gold"}

PG_HOST     = os.environ.get("POSTGRES_HOST_EXTERNAL", "localhost")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB       = os.environ.get("POSTGRES_DB", "app")
PG_USER     = os.environ.get("POSTGRES_USER", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL", "localhost:19092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC_NOTIFICACOES", "notificacoes.raw")
KAFKA_DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_DLQ", "notificacoes.dlq")

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8085"))

FIXTURES_DIR = Path(__file__).parents[1] / "fixtures"
KEEP_COMPOSE = os.environ.get("KEEP_COMPOSE", "0") == "1"

SMOKE_TIMEOUT = int(os.environ.get("SMOKE_TIMEOUT", "120"))  # seconds


# ── Helpers ───────────────────────────────────────────────────────────────────

def wait_for(condition_fn, timeout: int = 30, interval: float = 2.0, label: str = "") -> None:
    """Waits until condition_fn() returns True or timeout expires."""
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            if condition_fn():
                return
        except Exception as e:
            last_exc = e
        time.sleep(interval)
    msg = f"Timeout ({timeout}s) waiting for: {label or condition_fn.__name__}"
    if last_exc:
        msg += f" — last error: {last_exc}"
    raise TimeoutError(msg)


def pg_conn(user: str = PG_USER, password: str = PG_PASSWORD):
    """Returns a psycopg2 connection. Lazy import to avoid breaking unit test imports."""
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=user, password=password,
        connect_timeout=10,
    )


def minio_client():
    """Returns a boto3 S3 client configured for local MinIO."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


# ── Pytest fixtures ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def compose_up():
    """Brings up docker compose core + streaming if KEEP_COMPOSE=0."""
    if KEEP_COMPOSE:
        yield
        return

    subprocess.run(
        ["docker", "compose", "--profile", "core", "--profile", "streaming", "up", "-d"],
        check=True, capture_output=True,
    )

    # Wait for Postgres and MinIO
    wait_for(
        lambda: _pg_ready(),
        timeout=90, label="postgres ready",
    )
    wait_for(
        lambda: _minio_ready(),
        timeout=90, label="minio ready",
    )

    yield

    if os.environ.get("TEARDOWN_COMPOSE", "1") == "1":
        subprocess.run(
            ["docker", "compose", "--profile", "core", "--profile", "streaming", "down"],
            check=False, capture_output=True,
        )


def _pg_ready() -> bool:
    try:
        conn = pg_conn()
        conn.close()
        return True
    except Exception:
        return False


def _minio_ready() -> bool:
    import requests
    resp = requests.head(f"{MINIO_ENDPOINT}/minio/health/live", timeout=5)
    return resp.status_code == 200


@pytest.fixture(scope="session")
def s3(compose_up):
    return minio_client()


@pytest.fixture(scope="session")
def pg(compose_up):
    conn = pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def pg_gold(compose_up):
    conn = pg_conn(
        user=os.environ.get("POSTGRES_GOLD_USER", "gold_engineer"),
        password=os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer"),
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def seed_oltp(pg, compose_up):
    """Populates oltp.paciente with deterministic data (seed=42) if still empty."""
    cur = pg.cursor()
    cur.execute("SELECT COUNT(*) FROM oltp.paciente")
    count = cur.fetchone()[0]
    if count == 0:
        seed_sql = FIXTURES_DIR / "oltp_seed.sql"
        if seed_sql.exists():
            cur.execute(seed_sql.read_text())
            pg.commit()
    cur.close()
    return count or _count_oltp(pg)


def _count_oltp(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM oltp.paciente")
    n = cur.fetchone()[0]
    cur.close()
    return n


@pytest.fixture(scope="session")
def kafka_producer(compose_up):
    """Returns a confluent_kafka Producer configured for the local broker."""
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "1"})
    yield p
    p.flush(timeout=10)


@pytest.fixture(scope="module")
def spark_session(compose_up):
    """Local SparkSession with Delta + S3A pointing to MinIO."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("smoke_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "2g")
        .master("local[2]")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
