"""Smoke 02 — Bronze ingestion (OFFLINE_MODE with local fixture).

Uses OFFLINE_MODE=1 to read from tests/fixtures/ instead of calling real APIs.
Verifies schema, row count, partition, and metadata after Delta write.

Target time: < 90s (includes local Spark startup)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.smoke.conftest import FIXTURES_DIR

pytestmark = pytest.mark.smoke


def test_fixtures_exist():
    """Development fixtures exist before running any pipeline."""
    for fname in ["dengue_sample.json", "cnes_sample.json", "oltp_seed.sql"]:
        path = FIXTURES_DIR / fname
        assert path.exists(), f"Missing fixture: {path}"


def _load_jsonl(path: Path) -> list[dict]:
    """Reads a JSONL (one JSON object per line) fixture file."""
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_dengue_fixture_has_minimum_fields():
    fixture = FIXTURES_DIR / "dengue_sample.json"
    records = _load_jsonl(fixture)
    assert len(records) >= 10, "Dengue fixture must have >= 10 records"
    required = {"nu_ano", "sg_uf_not"}
    for rec in records[:3]:
        missing = required - set(rec.keys())
        assert not missing, f"Missing fields in dengue fixture: {missing}"


def test_cnes_fixture_has_minimum_fields():
    fixture = FIXTURES_DIR / "cnes_sample.json"
    records = _load_jsonl(fixture)
    assert len(records) >= 5, "CNES fixture must have >= 5 records"
    required = {"codigo_cnes", "nome_fantasia", "codigo_municipio"}
    for rec in records[:3]:
        missing = required - set(rec.keys())
        assert not missing, f"Missing fields in CNES fixture: {missing}"


def test_bronze_arboviroses_offline(spark_session, s3, tmp_path):
    """Bronze dengue pipeline in offline mode writes a valid Delta table."""
    from apps.epidemiologico.jobs.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = "s3a://bronze/smoke_test_dengue/"

    job = ArbovirosesBronzeJob()
    count = job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="smoke-01",
        source_url="smoke://fixture/dengue",
    )

    assert count > 0, "Bronze job must write >= 1 row"

    # Verify that the Delta table is readable and has metadata columns
    df = spark_session.read.format("delta").load(output)
    assert df.count() == count
    assert "_ingestion_ts" in df.columns
    assert "_batch_id" in df.columns
    assert "_source_url" in df.columns
    assert "ano_mes" in df.columns


def test_bronze_cnes_offline(spark_session, s3):
    """Bronze CNES pipeline in offline mode writes a Delta table with snapshot_date partition."""
    from apps.hospitalar.jobs.bronze_cnes import CnesBronzeJob

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
    """All Bronze metadata columns must be fully populated (no nulls)."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load("s3a://bronze/smoke_test_dengue/")

    for col in ("_ingestion_ts", "_batch_id", "_source_url"):
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Column {col} has {null_count} nulls"


def test_bronze_idempotency(spark_session, s3):
    """Re-running the same Bronze job does not change the row count or create duplicates."""
    from apps.epidemiologico.jobs.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = "s3a://bronze/smoke_test_dengue/"

    count_before = spark_session.read.format("delta").load(output).count()

    # Second run with the same partition_val
    ArbovirosesBronzeJob().run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="smoke-01-retry",
        source_url="smoke://fixture/dengue",
    )

    count_after = spark_session.read.format("delta").load(output).count()
    assert count_after == count_before, f"Idempotency failed: {count_before} -> {count_after} rows"
