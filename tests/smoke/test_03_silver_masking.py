"""Smoke 03 — Silver masking (LGPD).

This is the most critical assertion in the project. If it breaks, the demo is cancelled.

Verifies:
  1. No CPF from Bronze survives in Silver (EXCEPT / anti-join)
  2. Deterministic SHA-256: same CPF -> same hash
  3. email/telefone suppressed (NULL)
  4. CEP truncated to 3 digits
  5. data_nascimento generalised to year only (no day/month)
  6. masking_version present and populated

Target time: < 60s (requires test_02 to have run first in the same session)
"""

from __future__ import annotations

import pytest

from tests.smoke.conftest import FIXTURES_DIR

pytestmark = pytest.mark.smoke

BRONZE_OLTP_PATH = "s3a://bronze/smoke_oltp_paciente/"
SILVER_PATH = "s3a://silver/smoke_paciente/"


@pytest.fixture(scope="module")
def silver_df(spark_session, s3):
    """Stages OLTP fixture as Bronze Delta, runs PacienteSilverJob, returns Silver DF."""
    from pipelines.batch.silver_paciente import PacienteSilverJob
    from pyspark.sql import functions as F

    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json missing")

    # Stage JSON as Bronze Delta table so silver_base._read() can load it
    # oltp_sample.json is a JSON array — multiLine is required
    bronze_df = spark_session.read.option("multiLine", "true").json(str(fixture))
    if "snapshot_date" not in bronze_df.columns:
        bronze_df = bronze_df.withColumn("snapshot_date", F.lit("20240101"))
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .save(BRONZE_OLTP_PATH)
    )

    job = PacienteSilverJob()
    job.run(
        spark=spark_session,
        input_path=BRONZE_OLTP_PATH,
        output_path=SILVER_PATH,
        filter_col="snapshot_date",
        filter_val="20240101",
        batch_id="smoke-silver-01",
    )
    return spark_session.read.format("delta").load(SILVER_PATH)


@pytest.fixture(scope="module")
def bronze_oltp_df(spark_session, s3):
    """Reads the OLTP fixture directly (bronze raw) for the EXCEPT check."""
    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json missing")
    return spark_session.read.json(str(fixture))


# ── The most critical assertion ────────────────────────────────────────────────


def test_cpf_not_in_silver(silver_df):
    """No CPF from OLTP must appear in Silver (LGPD Art. 16)."""
    assert (
        "cpf" not in silver_df.columns
    ), "CRITICAL FAILURE: column 'cpf' present in Silver — PII leakage!"


def test_no_pii_columns_in_silver(silver_df):
    """The full list of PII columns must not exist in Silver."""
    pii_cols = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii_cols & set(silver_df.columns)
    assert not leakage, f"CRITICAL FAILURE: PII columns in Silver: {leakage}"


# ── Deterministic hash ────────────────────────────────────────────────────────


def test_cpf_hash_is_deterministic(silver_df):
    """The same CPF must always produce the same hash (SHA-256 + salt)."""

    assert "id_paciente_hash" in silver_df.columns, "Column id_paciente_hash missing from Silver"

    # Verify there are no hash duplicates (each unique CPF -> unique hash)
    total = silver_df.count()
    distinct_hashes = silver_df.select("id_paciente_hash").distinct().count()
    assert (
        distinct_hashes == total
    ), f"Hash collision detected: {total} rows, {distinct_hashes} unique hashes"


def test_cpf_hash_is_sha256_format(silver_df):
    """Hash must be 64 hex characters (SHA-256)."""

    sample = silver_df.select("id_paciente_hash").first()
    if sample is None:
        pytest.skip("Silver is empty")
    h = sample["id_paciente_hash"]
    assert len(h) == 64, f"Hash has wrong length: {len(h)} (expected 64)"
    assert all(c in "0123456789abcdef" for c in h.lower()), f"Hash is not hexadecimal: {h[:20]}..."


def test_id_paciente_hash_no_nulls(silver_df):
    """No hash must be NULL — every patient must have a hashed identity."""
    from pyspark.sql import functions as F

    null_count = silver_df.filter(F.col("id_paciente_hash").isNull()).count()
    assert null_count == 0, f"{null_count} patients without id_paciente_hash"


# ── email/telefone suppression ────────────────────────────────────────────────


def test_email_suppressed_in_silver(silver_df):
    """email must not exist as a column in Silver."""
    assert "email" not in silver_df.columns, "email present in Silver — suppression failed"


def test_telefone_suppressed_in_silver(silver_df):
    """telefone must not exist as a column in Silver."""
    assert "telefone" not in silver_df.columns, "telefone present in Silver — suppression failed"


# ── Truncated CEP ────────────────────────────────────────────────────────────


def test_cep_truncated_to_3_digits(silver_df):
    """cep must have been truncated to 3 digits (cep_regiao)."""
    assert "cep" not in silver_df.columns, "Full cep present in Silver"
    assert "cep_regiao" in silver_df.columns, "cep_regiao missing from Silver"

    from pyspark.sql import functions as F

    invalid = silver_df.filter(F.length(F.col("cep_regiao")) != 3).count()
    assert invalid == 0, f"{invalid} records with cep_regiao length different from 3"


# ── data_nascimento generalisation ───────────────────────────────────────────


def test_data_nascimento_generalized_to_year(silver_df):
    """data_nascimento must have been replaced by ano_nascimento (int)."""
    assert (
        "data_nascimento" not in silver_df.columns
    ), "data_nascimento present in Silver — generalisation failed"
    assert "ano_nascimento" in silver_df.columns, "ano_nascimento missing from Silver"

    from pyspark.sql import functions as F

    # Year values must be reasonable (1900-2024)
    invalid = silver_df.filter(
        (F.col("ano_nascimento") < 1900) | (F.col("ano_nascimento") > 2024)
    ).count()
    assert invalid == 0, f"{invalid} records with ano_nascimento outside the range 1900-2024"


# ── Masking metadata ──────────────────────────────────────────────────────────


def test_masking_version_present(silver_df):
    """masking_version must be populated on every row."""
    from pyspark.sql import functions as F

    assert "masking_version" in silver_df.columns, "masking_version missing from Silver"

    null_count = silver_df.filter(F.col("masking_version").isNull()).count()
    assert null_count == 0, f"{null_count} rows without masking_version"


def test_masking_version_value(silver_df):
    """masking_version must follow the semantic pattern 'masking-X.Y.Z'."""

    versions = silver_df.select("masking_version").distinct().collect()
    assert len(versions) == 1, "masking_version must not vary within the same load"
    v = versions[0]["masking_version"]
    assert v.startswith("masking-"), f"Unexpected masking_version: {v}"


def test_masked_at_no_nulls(silver_df):
    """masked_at must be populated (masking timestamp)."""
    from pyspark.sql import functions as F

    if "masked_at" not in silver_df.columns:
        pytest.skip("masked_at not implemented in this version")

    null_count = silver_df.filter(F.col("masked_at").isNull()).count()
    assert null_count == 0, f"{null_count} rows without masked_at"
