"""Smoke 08 — Invariantes de Segurança.

Verifica que a infraestrutura e o código atendem requisitos mínimos de segurança:
  1. Nenhum bucket MinIO com acesso público
  2. PII_SALT não está hardcoded nos arquivos fonte
  3. k8s secrets não têm PII_SALT em plaintext nos yamls commitados
  4. Buckets existem e têm naming correto
  5. PII scan no Silver: nenhuma coluna PII sobreviveu

Tempo alvo: < 30s
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from tests.smoke.conftest import MINIO_BUCKETS, minio_client

pytestmark = pytest.mark.smoke

# Raiz do repositório
REPO_ROOT = Path(__file__).parents[3]

# Colunas PII proibidas em qualquer path Silver ou Gold
_PII_COLS = {"cpf", "rg", "nome_completo", "data_nascimento", "email_pessoal", "telefone"}

# Arquivos onde PII é permitido (por necessidade de negócio documentada)
_PII_ALLOWED_PATHS = {
    "pipelines/extraction/oltp_snapshot.py",
    "pipelines/common/masking.py",
}


# ── Segurança MinIO ────────────────────────────────────────────────────────────

def test_no_public_bucket_policies(compose_up):
    """Nenhum bucket deve ter política pública (AllUsers)."""
    s3 = minio_client()
    for bucket in MINIO_BUCKETS:
        try:
            policy = s3.get_bucket_policy(Bucket=bucket)
            policy_str = str(policy.get("Policy", ""))
            assert "AllUsers" not in policy_str, \
                f"FALHA DE SEGURANÇA: bucket '{bucket}' tem acesso público (AllUsers)"
        except Exception as e:
            if "NoSuchBucketPolicy" in str(e):
                pass  # sem policy = acesso privado ✓
            else:
                raise


def test_buckets_use_correct_naming(compose_up):
    """Buckets devem seguir nomenclatura: landing, bronze, silver, gold."""
    s3 = minio_client()
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    expected = {"landing", "bronze", "silver", "gold"}
    # Não deve haver buckets extras não esperados (ex.: "test-bucket-public")
    unexpected = existing - expected - {"logs", "checkpoints"}  # buckets auxiliares ok
    assert not unexpected, f"Buckets inesperados detectados: {unexpected}"


# ── Segurança de credenciais no código ────────────────────────────────────────

def test_pii_salt_not_hardcoded_in_source():
    """PII_SALT não deve aparecer hardcoded em arquivos Python."""
    # Exemplos de valores que NÃO deveriam estar hardcoded
    suspicious_patterns = [
        r'PII_SALT\s*=\s*["\'][^"\']{8,}["\']',   # variável com valor
        r'salt\s*=\s*["\'][^"\']{8,}["\']',         # variável genérica salt
        r'secret.*=.*santander',                      # segredo específico do case
    ]

    py_files = list((REPO_ROOT / "pipelines").rglob("*.py"))
    py_files += list((REPO_ROOT / "airflow").rglob("*.py"))

    violations = []
    for f in py_files:
        content = f.read_text(errors="ignore")
        for pat in suspicious_patterns:
            if re.search(pat, content, re.IGNORECASE):
                violations.append(f"{f.relative_to(REPO_ROOT)}: matches '{pat}'")

    assert not violations, "Credenciais hardcoded detectadas:\n" + "\n".join(violations)


def test_no_pii_column_names_outside_allowed_files():
    """Nomes de colunas PII só podem aparecer em arquivos autorizados."""
    # Padrão: aspas em torno do nome da coluna (coluna literal em código)
    pii_pattern = re.compile(
        r'["\'](' + "|".join(_PII_COLS) + r')["\']'
    )

    violations = []
    for py_file in (REPO_ROOT / "pipelines").rglob("*.py"):
        rel = str(py_file.relative_to(REPO_ROOT))
        if any(rel.endswith(allowed) for allowed in _PII_ALLOWED_PATHS):
            continue
        content = py_file.read_text(errors="ignore")
        if pii_pattern.search(content):
            violations.append(rel)

    assert not violations, \
        "Referências a colunas PII fora dos arquivos autorizados:\n" + "\n".join(violations)


def test_no_plaintext_passwords_in_python():
    """Senhas e secrets não devem aparecer hardcoded em .py."""
    password_pattern = re.compile(
        r'(password|passwd|secret|token)\s*=\s*["\'][^"\']{6,}["\']',
        re.IGNORECASE,
    )
    # Exceções conhecidas: arquivos de teste com valores fake
    allowed_test_values = {"postgres", "minioadmin", "localhost", "gold_engineer"}

    violations = []
    for py_file in (REPO_ROOT / "pipelines").rglob("*.py"):
        content = py_file.read_text(errors="ignore")
        for match in password_pattern.finditer(content):
            value_part = match.group(0).split("=", 1)[1].strip().strip("'\"")
            if value_part not in allowed_test_values:
                violations.append(
                    f"{py_file.relative_to(REPO_ROOT)}: {match.group(0)[:60]}"
                )

    assert not violations, \
        "Senhas hardcoded detectadas:\n" + "\n".join(violations)


# ── PII scan no Silver ────────────────────────────────────────────────────────

def test_silver_pii_scan_filesystem():
    """Nenhum arquivo Parquet/Delta no diretório Silver local deve ter nome de coluna PII."""
    silver_local = REPO_ROOT / "data" / "silver"
    if not silver_local.exists():
        pytest.skip("data/silver/ não existe localmente — skip filesystem scan")

    parquet_files = list(silver_local.rglob("*.parquet"))
    if not parquet_files:
        pytest.skip("Nenhum arquivo Parquet em data/silver/")

    try:
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow não instalado — skip Parquet scan")

    violations = []
    for pf in parquet_files[:10]:  # amostra: 10 arquivos máximo
        schema = pq.read_schema(pf)
        col_names = set(schema.names)
        leaked = col_names & _PII_COLS
        if leaked:
            violations.append(f"{pf.relative_to(REPO_ROOT)}: {leaked}")

    assert not violations, \
        "PII detectado em arquivos Silver Parquet:\n" + "\n".join(violations)


# ── Kubernetes secrets ────────────────────────────────────────────────────────

def test_k8s_secret_yaml_does_not_expose_prod_values():
    """k8s/*.yaml não devem conter valores de produção reais."""
    k8s_dir = REPO_ROOT / "k8s"
    if not k8s_dir.exists():
        pytest.skip("Diretório k8s/ não encontrado")

    prod_indicators = [
        r'\bpassword\s*:\s*[^\s]{12,}',   # senha longa (prod)
        r'arn:aws:',                        # ARN real da AWS
        r'AKIA[A-Z0-9]{16}',              # AWS access key
    ]

    violations = []
    for yaml_file in k8s_dir.rglob("*.yaml"):
        content = yaml_file.read_text(errors="ignore")
        for pat in prod_indicators:
            if re.search(pat, content):
                violations.append(
                    f"{yaml_file.relative_to(REPO_ROOT)}: matches '{pat}'"
                )

    assert not violations, \
        "Valores sensíveis de produção em k8s/:\n" + "\n".join(violations)
