"""Smoke 06 — Pipeline idempotency.

Re-runs each job (Bronze and Silver) with the same partition_val
and verifies that the final row count does not change.

Principle: Delta pipelines using replaceWhere must be safe to re-run.

Target time: < 90s
"""

from __future__ import annotations

import pytest

from tests.smoke.conftest import FIXTURES_DIR

pytestmark = pytest.mark.smoke


def _count(spark, path: str) -> int:
    return spark.read.format("delta").load(path).count()


# ── Bronze Arboviroses ─────────────────────────────────────────────────────────


def test_bronze_arboviroses_idempotent(spark_session, s3):
    """Three runs of the Bronze dengue job with the same partition_val -> same count."""
    from apps.epidemiologico.jobs.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = "s3a://bronze/idempotency_dengue/"

    job = ArbovirosesBronzeJob()
    common = dict(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        source_url="smoke://idempotency/dengue",
    )

    job.run(**common, batch_id="idem-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-02")
    count_2 = _count(spark_session, output)

    job.run(**common, batch_id="idem-03")
    count_3 = _count(spark_session, output)

    assert (
        count_1 == count_2 == count_3
    ), f"Bronze arboviroses not idempotent: {count_1} -> {count_2} -> {count_3}"


# ── Bronze CNES ───────────────────────────────────────────────────────────────


def test_bronze_cnes_idempotent(spark_session, s3):
    """Three runs of the Bronze CNES job -> same count."""
    from apps.hospitalar.jobs.bronze_cnes import CnesBronzeJob

    fixture = FIXTURES_DIR / "cnes_sample.json"
    output = "s3a://bronze/idempotency_cnes/"

    job = CnesBronzeJob()
    common = dict(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="20240101",
        source_url="smoke://idempotency/cnes",
    )

    job.run(**common, batch_id="idem-cnes-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-cnes-02")
    count_2 = _count(spark_session, output)

    assert count_1 == count_2, f"Bronze CNES not idempotent: {count_1} -> {count_2}"


# ── Silver Paciente ────────────────────────────────────────────────────────────


def test_silver_paciente_idempotent(spark_session, s3):
    """Two runs of the Silver paciente job -> same count."""
    from apps.epidemiologico.jobs.silver_paciente import PacienteSilverJob
    from pyspark.sql import functions as F

    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json missing")

    # Stage as Bronze Delta (oltp_sample.json is a JSON array, not Delta)
    bronze_path = "s3a://bronze/idempotency_oltp_paciente/"
    bronze_df = spark_session.read.option("multiLine", "true").json(str(fixture))
    if "snapshot_date" not in bronze_df.columns:
        bronze_df = bronze_df.withColumn("snapshot_date", F.lit("20240101"))
    bronze_df.write.format("delta").mode("overwrite").partitionBy("snapshot_date").save(bronze_path)

    output = "s3a://silver/idempotency_paciente/"
    job = PacienteSilverJob()
    common = dict(
        spark=spark_session,
        input_path=bronze_path,
        output_path=output,
        filter_col="snapshot_date",
        filter_val="20240101",
    )

    job.run(**common, batch_id="idem-silver-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-silver-02")
    count_2 = _count(spark_session, output)

    assert count_1 == count_2, f"Silver paciente not idempotent: {count_1} -> {count_2}"


# ── Multiple partitions do not interfere ──────────────────────────────────────


def test_different_partitions_accumulate(spark_session, s3):
    """Distinct partitions must accumulate (not overwrite) in Delta."""
    from apps.epidemiologico.jobs.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output = "s3a://bronze/idempotency_multi_partition/"

    # Clean up any residual data from previous test runs to ensure isolation
    try:
        for obj in s3.list_objects_v2(Bucket="bronze", Prefix="idempotency_multi_partition/").get(
            "Contents", []
        ):
            s3.delete_object(Bucket="bronze", Key=obj["Key"])
    except Exception:
        pass

    job = ArbovirosesBronzeJob()

    job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="multi-01",
        source_url="smoke://idempotency/multi",
    )
    count_jan = _count(spark_session, output)

    job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202402",
        batch_id="multi-02",
        source_url="smoke://idempotency/multi",
    )
    count_jan_feb = _count(spark_session, output)

    assert (
        count_jan_feb == count_jan * 2
    ), f"Partitions should accumulate: 1x{count_jan} + 1x{count_jan} != {count_jan_feb}"
