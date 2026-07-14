"""Smoke 07 — Quality Gates (Great Expectations).

Runs GE checkpoints on the Bronze and Silver data already written
by the previous tests. Fails if critical expectations do not pass.

Dependency: test_02 and test_03 must have run first (same session).

Target time: < 60s
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.smoke

BRONZE_DENGUE_PATH = "s3a://bronze/smoke_test_dengue/"
SILVER_PACIENTE_PATH = "s3a://silver/smoke_paciente/"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _ge_context():
    """Returns the Great Expectations DataContext only if checkpoints are configured."""
    try:
        from pathlib import Path

        import great_expectations as gx

        ctx = gx.get_context()
        # Only use GE if at least one expected checkpoint YAML exists
        cp_dir = Path(ctx.root_directory) / "checkpoints"
        if not any(cp_dir.glob("*.yml")):
            return None
        return ctx
    except Exception:
        return None


# ── Bronze completeness ────────────────────────────────────────────────────────


def test_bronze_dengue_no_null_ano(spark_session):
    """nu_ano must not be null in the Bronze dengue table."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    nulls = df.filter(F.col("nu_ano").isNull()).count()
    assert nulls == 0, f"{nulls} records with null nu_ano in Bronze dengue"


def test_bronze_dengue_uf_not_valid(spark_session):
    """sg_uf_not must be a 2-letter state code."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    invalid = df.filter(F.length(F.col("sg_uf_not")) != 2).count()
    assert invalid == 0, f"{invalid} records with invalid sg_uf_not"


def test_bronze_dengue_ano_range(spark_session):
    """nu_ano must be within a reasonable range (2000-2030)."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    out_of_range = df.filter((F.col("nu_ano") < 2000) | (F.col("nu_ano") > 2030)).count()
    assert out_of_range == 0, f"{out_of_range} records with nu_ano outside the range 2000-2030"


def test_bronze_metadata_completeness(spark_session):
    """All Bronze metadata columns must be 100% populated."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    for col in ("_ingestion_ts", "_batch_id", "_source_url", "ano_mes"):
        pct_null = df.filter(F.col(col).isNull()).count() / df.count()
        assert pct_null == 0.0, f"Column {col} has {pct_null:.1%} nulls"


# ── Silver completeness ────────────────────────────────────────────────────────


def test_silver_paciente_hash_uniqueness(spark_session):
    """id_paciente_hash must be unique in Silver (no collisions)."""
    fixture_path = SILVER_PACIENTE_PATH
    try:
        df = spark_session.read.format("delta").load(fixture_path)
    except Exception:
        pytest.skip("Silver paciente not available — run test_03 first")

    total = df.count()
    distinct = df.select("id_paciente_hash").distinct().count()
    assert total == distinct, f"Hash collision in Silver: {total} rows, {distinct} unique hashes"


def test_silver_ano_nascimento_completeness(spark_session):
    """ano_nascimento must not have nulls in Silver."""
    from pyspark.sql import functions as F

    try:
        df = spark_session.read.format("delta").load(SILVER_PACIENTE_PATH)
    except Exception:
        pytest.skip("Silver paciente not available")

    nulls = df.filter(F.col("ano_nascimento").isNull()).count()
    assert nulls == 0, f"{nulls} records without ano_nascimento in Silver"


def test_silver_cep_regiao_format(spark_session):
    """cep_regiao must have exactly 3 digits."""
    from pyspark.sql import functions as F

    try:
        df = spark_session.read.format("delta").load(SILVER_PACIENTE_PATH)
    except Exception:
        pytest.skip("Silver paciente not available")

    invalid = df.filter(F.col("cep_regiao").isNull() | (F.length(F.col("cep_regiao")) != 3)).count()
    assert invalid == 0, f"{invalid} records with invalid cep_regiao"


# ── Great Expectations checkpoint (optional) ──────────────────────────────────


@pytest.mark.skipif(
    _ge_context() is None,
    reason="Great Expectations not configured in this environment",
)
def test_ge_bronze_checkpoint(spark_session):
    """Runs the GE checkpoint for Bronze dengue if available."""
    import great_expectations as gx

    ctx = gx.get_context()
    result = ctx.run_checkpoint(checkpoint_name="bronze_dengue_checkpoint")
    assert result.success, f"Great Expectations checkpoint failed: {result.statistics}"


@pytest.mark.skipif(
    _ge_context() is None,
    reason="Great Expectations not configured in this environment",
)
def test_ge_silver_checkpoint(spark_session):
    """Runs the GE checkpoint for Silver paciente if available."""
    import great_expectations as gx

    ctx = gx.get_context()
    result = ctx.run_checkpoint(checkpoint_name="silver_paciente_checkpoint")
    assert result.success, f"Great Expectations checkpoint failed: {result.statistics}"
