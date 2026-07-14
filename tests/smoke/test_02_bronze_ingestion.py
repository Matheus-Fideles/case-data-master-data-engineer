"""Smoke 02 — Ingestão Bronze (OFFLINE_MODE com fixture local).

Usa OFFLINE_MODE=1 para ler de tests/fixtures/ em vez de chamar APIs reais.
Verifica schema, contagem, partição e metadados após escrita Delta.

Tempo alvo: < 90s (inclui startup do Spark local)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.smoke.conftest import FIXTURES_DIR, MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET

pytestmark = pytest.mark.smoke


def test_fixtures_exist():
    """Fixtures de desenvolvimento existem antes de rodar qualquer pipeline."""
    for fname in ["dengue_sample.json", "cnes_sample.json", "oltp_seed.sql"]:
        path = FIXTURES_DIR / fname
        assert path.exists(), f"Fixture ausente: {path}"


def test_dengue_fixture_has_minimum_fields():
    fixture = FIXTURES_DIR / "dengue_sample.json"
    records = json.loads(fixture.read_text())
    assert len(records) >= 10, "Fixture dengue deve ter ≥ 10 registros"
    required = {"nu_ano", "sg_uf_not"}
    for rec in records[:3]:
        missing = required - set(rec.keys())
        assert not missing, f"Campos ausentes na fixture dengue: {missing}"


def test_cnes_fixture_has_minimum_fields():
    fixture = FIXTURES_DIR / "cnes_sample.json"
    records = json.loads(fixture.read_text())
    assert len(records) >= 5, "Fixture CNES deve ter ≥ 5 registros"
    required = {"codigo_cnes", "nome_fantasia", "codigo_municipio"}
    for rec in records[:3]:
        missing = required - set(rec.keys())
        assert not missing, f"Campos ausentes na fixture CNES: {missing}"


def test_bronze_arboviroses_offline(spark_session, s3, tmp_path):
    """Pipeline Bronze dengue em modo offline escreve Delta válido."""
    from pyspark.sql import functions as F
    from pipelines.batch.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = f"s3a://bronze/smoke_test_dengue/"

    job = ArbovirosesBronzeJob()
    count = job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="smoke-01",
        source_url="smoke://fixture/dengue",
    )

    assert count > 0, "Bronze job deve escrever ≥ 1 linha"

    # Verifica que o Delta é legível e tem metadados
    df = spark_session.read.format("delta").load(output)
    assert df.count() == count
    assert "_ingestion_ts" in df.columns
    assert "_batch_id" in df.columns
    assert "_source_url" in df.columns
    assert "ano_mes" in df.columns


def test_bronze_cnes_offline(spark_session, s3):
    """Pipeline Bronze CNES em modo offline escreve Delta com partição snapshot_date."""
    from pipelines.batch.bronze_cnes import CnesBronzeJob

    fixture = FIXTURES_DIR / "cnes_sample.json"
    output = "s3a://bronze/smoke_test_cnes/"

    job = CnesBronzeJob()
    count = job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="20240101",
        batch_id="smoke-02",
        source_url="smoke://fixture/cnes",
    )

    assert count > 0
    df = spark_session.read.format("delta").load(output)
    assert "snapshot_date" in df.columns
    assert df.filter(df.snapshot_date == "20240101").count() == count


def test_bronze_metadata_columns_are_populated(spark_session, s3):
    """Todas as colunas de metadados Bronze devem estar preenchidas (sem nulos)."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load("s3a://bronze/smoke_test_dengue/")

    for col in ("_ingestion_ts", "_batch_id", "_source_url"):
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Coluna {col} tem {null_count} nulos"


def test_bronze_idempotency(spark_session, s3):
    """Reexecutar o mesmo job Bronze não muda count nem cria duplicatas."""
    from pipelines.batch.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = "s3a://bronze/smoke_test_dengue/"

    count_before = spark_session.read.format("delta").load(output).count()

    # Segunda execução com mesmo partition_val
    ArbovirosesBronzeJob().run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="smoke-01-retry",
        source_url="smoke://fixture/dengue",
    )

    count_after = spark_session.read.format("delta").load(output).count()
    assert count_after == count_before, \
        f"Idempotência falhou: {count_before} → {count_after} linhas"
