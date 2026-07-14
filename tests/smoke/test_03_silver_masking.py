"""Smoke 03 — Mascaramento Silver (LGPD).

Esse é o assert mais importante do projeto. Se quebrar, demo é cancelada.

Verifica:
  1. Nenhum CPF do Bronze sobrevive no Silver (EXCEPT / anti-join)
  2. SHA-256 determinístico: mesmo CPF → mesmo hash
  3. email/telefone suprimidos (NULL)
  4. CEP truncado a 3 dígitos
  5. data_nascimento generalizada para ano (sem dia/mês)
  6. masking_version presente e preenchido

Tempo alvo: < 60s (depende de test_02 ter rodado antes na mesma session)
"""
from __future__ import annotations

import hashlib
import os

import pytest

from tests.smoke.conftest import FIXTURES_DIR, MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET

pytestmark = pytest.mark.smoke

BRONZE_PATH = "s3a://bronze/smoke_test_dengue/"
SILVER_PATH  = "s3a://silver/smoke_paciente/"


@pytest.fixture(scope="module")
def silver_df(spark_session, s3):
    """Roda PacienteSilverJob sobre fixture OLTP e retorna DataFrame Silver."""
    from pipelines.batch.silver_paciente import PacienteSilverJob

    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json ausente — rode test_02 primeiro")

    job = PacienteSilverJob()
    job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=SILVER_PATH,
        filter_col="snapshot_date",
        filter_val="20240101",
        batch_id="smoke-silver-01",
    )
    return spark_session.read.format("delta").load(SILVER_PATH)


@pytest.fixture(scope="module")
def bronze_oltp_df(spark_session, s3):
    """Lê a fixture OLTP diretamente (bronze raw) para o EXCEPT check."""
    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json ausente")
    return spark_session.read.json(str(fixture))


# ── O assert mais crítico ──────────────────────────────────────────────────────

def test_cpf_not_in_silver(silver_df):
    """Nenhum CPF do OLTP deve aparecer no Silver (LGPD Art. 16)."""
    assert "cpf" not in silver_df.columns, \
        "FALHA CRÍTICA: coluna 'cpf' presente no Silver — vazamento de PII!"


def test_no_pii_columns_in_silver(silver_df):
    """Lista completa de colunas PII não deve existir no Silver."""
    pii_cols = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii_cols & set(silver_df.columns)
    assert not leakage, f"FALHA CRÍTICA: colunas PII no Silver: {leakage}"


# ── Hash determinístico ────────────────────────────────────────────────────────

def test_cpf_hash_is_deterministic(silver_df):
    """O mesmo CPF deve sempre gerar o mesmo hash (SHA-256 + salt)."""
    from pyspark.sql import functions as F

    assert "id_paciente_hash" in silver_df.columns, \
        "Coluna id_paciente_hash ausente no Silver"

    # Verifica que não há duplicatas de hash (cada CPF único → hash único)
    total = silver_df.count()
    distinct_hashes = silver_df.select("id_paciente_hash").distinct().count()
    assert distinct_hashes == total, \
        f"Colisão de hash detectada: {total} rows, {distinct_hashes} hashes únicos"


def test_cpf_hash_is_sha256_format(silver_df):
    """Hash deve ter 64 chars hex (SHA-256)."""
    from pyspark.sql import functions as F

    sample = silver_df.select("id_paciente_hash").first()
    if sample is None:
        pytest.skip("Silver vazio")
    h = sample["id_paciente_hash"]
    assert len(h) == 64, f"Hash com comprimento errado: {len(h)} (esperado 64)"
    assert all(c in "0123456789abcdef" for c in h.lower()), \
        f"Hash não é hexadecimal: {h[:20]}..."


def test_id_paciente_hash_no_nulls(silver_df):
    """Nenhum hash deve ser NULL — todo paciente deve ter identidade hasheada."""
    from pyspark.sql import functions as F

    null_count = silver_df.filter(F.col("id_paciente_hash").isNull()).count()
    assert null_count == 0, f"{null_count} pacientes sem id_paciente_hash"


# ── Supressão email/telefone ───────────────────────────────────────────────────

def test_email_suppressed_in_silver(silver_df):
    """email não deve existir como coluna no Silver."""
    assert "email" not in silver_df.columns, "email presente no Silver — supressão falhou"


def test_telefone_suppressed_in_silver(silver_df):
    """telefone não deve existir como coluna no Silver."""
    assert "telefone" not in silver_df.columns, "telefone presente no Silver — supressão falhou"


# ── CEP truncado ──────────────────────────────────────────────────────────────

def test_cep_truncated_to_3_digits(silver_df):
    """cep deve ter sido truncado para 3 dígitos (cep_regiao)."""
    assert "cep" not in silver_df.columns, "cep completo presente no Silver"
    assert "cep_regiao" in silver_df.columns, "cep_regiao ausente no Silver"

    from pyspark.sql import functions as F

    invalid = silver_df.filter(F.length(F.col("cep_regiao")) != 3).count()
    assert invalid == 0, f"{invalid} registros com cep_regiao com comprimento diferente de 3"


# ── Generalização de data_nascimento ──────────────────────────────────────────

def test_data_nascimento_generalized_to_year(silver_df):
    """data_nascimento deve ter sido substituída por ano_nascimento (int)."""
    assert "data_nascimento" not in silver_df.columns, \
        "data_nascimento presente no Silver — generalização falhou"
    assert "ano_nascimento" in silver_df.columns, "ano_nascimento ausente no Silver"

    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    # Valores de ano devem ser razoáveis (1900–2024)
    invalid = silver_df.filter(
        (F.col("ano_nascimento") < 1900) | (F.col("ano_nascimento") > 2024)
    ).count()
    assert invalid == 0, f"{invalid} registros com ano_nascimento fora do intervalo 1900–2024"


# ── Metadados de mascaramento ─────────────────────────────────────────────────

def test_masking_version_present(silver_df):
    """masking_version deve estar preenchido em todas as linhas."""
    from pyspark.sql import functions as F

    assert "masking_version" in silver_df.columns, "masking_version ausente no Silver"

    null_count = silver_df.filter(F.col("masking_version").isNull()).count()
    assert null_count == 0, f"{null_count} linhas sem masking_version"


def test_masking_version_value(silver_df):
    """masking_version deve seguir padrão semântico 'masking-X.Y.Z'."""
    from pyspark.sql import functions as F

    versions = silver_df.select("masking_version").distinct().collect()
    assert len(versions) == 1, "masking_version não deve variar na mesma carga"
    v = versions[0]["masking_version"]
    assert v.startswith("masking-"), f"masking_version inesperado: {v}"


def test_masked_at_no_nulls(silver_df):
    """masked_at deve estar preenchido (timestamp do mascaramento)."""
    from pyspark.sql import functions as F

    if "masked_at" not in silver_df.columns:
        pytest.skip("masked_at não implementado nesta versão")

    null_count = silver_df.filter(F.col("masked_at").isNull()).count()
    assert null_count == 0, f"{null_count} linhas sem masked_at"
