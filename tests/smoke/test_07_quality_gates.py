"""Smoke 07 — Quality Gates (Great Expectations).

Roda checkpoints GE sobre os dados Bronze e Silver já escritos
nos testes anteriores. Falha se expectativas críticas não passarem.

Dependência: test_02 e test_03 devem ter rodado antes (mesma session).

Tempo alvo: < 60s
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.smoke

BRONZE_DENGUE_PATH  = "s3a://bronze/smoke_test_dengue/"
SILVER_PACIENTE_PATH = "s3a://silver/smoke_paciente/"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ge_context():
    """Retorna Great Expectations DataContext se disponível."""
    try:
        import great_expectations as gx
        return gx.get_context()
    except Exception:
        return None


# ── Bronze completeness ────────────────────────────────────────────────────────

def test_bronze_dengue_no_null_ano(spark_session):
    """nu_ano não deve ser nulo no Bronze dengue."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    nulls = df.filter(F.col("nu_ano").isNull()).count()
    assert nulls == 0, f"{nulls} registros com nu_ano nulo no Bronze dengue"


def test_bronze_dengue_uf_not_valid(spark_session):
    """sg_uf_not deve ser uma sigla de 2 letras."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    invalid = df.filter(F.length(F.col("sg_uf_not")) != 2).count()
    assert invalid == 0, f"{invalid} registros com sg_uf_not inválida"


def test_bronze_dengue_ano_range(spark_session):
    """nu_ano deve estar em intervalo razoável (2000–2030)."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    out_of_range = df.filter(
        (F.col("nu_ano") < 2000) | (F.col("nu_ano") > 2030)
    ).count()
    assert out_of_range == 0, f"{out_of_range} registros com nu_ano fora do intervalo 2000–2030"


def test_bronze_metadata_completeness(spark_session):
    """Todas as colunas de metadados Bronze devem estar 100% preenchidas."""
    from pyspark.sql import functions as F

    df = spark_session.read.format("delta").load(BRONZE_DENGUE_PATH)
    for col in ("_ingestion_ts", "_batch_id", "_source_url", "ano_mes"):
        pct_null = df.filter(F.col(col).isNull()).count() / df.count()
        assert pct_null == 0.0, f"Coluna {col} tem {pct_null:.1%} nulos"


# ── Silver completeness ────────────────────────────────────────────────────────

def test_silver_paciente_hash_uniqueness(spark_session):
    """id_paciente_hash deve ser único no Silver (sem colisões)."""
    fixture_path = SILVER_PACIENTE_PATH
    try:
        df = spark_session.read.format("delta").load(fixture_path)
    except Exception:
        pytest.skip("Silver paciente não disponível — rode test_03 antes")

    total   = df.count()
    distinct = df.select("id_paciente_hash").distinct().count()
    assert total == distinct, \
        f"Colisão de hash no Silver: {total} rows, {distinct} hashes únicos"


def test_silver_ano_nascimento_completeness(spark_session):
    """ano_nascimento não deve ter nulos no Silver."""
    from pyspark.sql import functions as F

    try:
        df = spark_session.read.format("delta").load(SILVER_PACIENTE_PATH)
    except Exception:
        pytest.skip("Silver paciente não disponível")

    nulls = df.filter(F.col("ano_nascimento").isNull()).count()
    assert nulls == 0, f"{nulls} registros sem ano_nascimento no Silver"


def test_silver_cep_regiao_format(spark_session):
    """cep_regiao deve ter exatamente 3 dígitos."""
    from pyspark.sql import functions as F

    try:
        df = spark_session.read.format("delta").load(SILVER_PACIENTE_PATH)
    except Exception:
        pytest.skip("Silver paciente não disponível")

    invalid = df.filter(
        F.col("cep_regiao").isNull() | (F.length(F.col("cep_regiao")) != 3)
    ).count()
    assert invalid == 0, f"{invalid} registros com cep_regiao inválido"


# ── Great Expectations checkpoint (opcional) ──────────────────────────────────

@pytest.mark.skipif(
    _ge_context() is None,
    reason="Great Expectations não configurado neste ambiente",
)
def test_ge_bronze_checkpoint(spark_session):
    """Roda checkpoint GE para Bronze dengue se disponível."""
    import great_expectations as gx

    ctx = gx.get_context()
    result = ctx.run_checkpoint(checkpoint_name="bronze_dengue_checkpoint")
    assert result.success, \
        f"Great Expectations checkpoint falhou: {result.statistics}"


@pytest.mark.skipif(
    _ge_context() is None,
    reason="Great Expectations não configurado neste ambiente",
)
def test_ge_silver_checkpoint(spark_session):
    """Roda checkpoint GE para Silver paciente se disponível."""
    import great_expectations as gx

    ctx = gx.get_context()
    result = ctx.run_checkpoint(checkpoint_name="silver_paciente_checkpoint")
    assert result.success, \
        f"Great Expectations checkpoint falhou: {result.statistics}"
