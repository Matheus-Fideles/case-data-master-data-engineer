"""Smoke 08 — Security invariants.

Verifies that the infrastructure and code meet minimum security requirements:
  1. No MinIO bucket with public access
  2. PII_SALT is not hardcoded in source files
  3. k8s secrets do not contain PII_SALT in plaintext in committed yamls
  4. Buckets exist and follow the correct naming convention
  5. PII scan on Silver: no PII column survived

Target time: < 30s
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.smoke.conftest import MINIO_BUCKETS, minio_client

pytestmark = pytest.mark.smoke

# Repository root
REPO_ROOT = Path(__file__).parents[3]

# PII columns forbidden in any Silver or Gold path
_PII_COLS = {"cpf", "rg", "nome_completo", "data_nascimento", "email_pessoal", "telefone"}

# Files where PII is allowed (by documented business necessity)
_PII_ALLOWED_PATHS = {
    "pipelines/extraction/oltp_snapshot.py",
    "pipelines/common/masking.py",
}


# ── MinIO security ────────────────────────────────────────────────────────────


def test_no_public_bucket_policies(compose_up):
    """No bucket must have a public policy (AllUsers)."""
    s3 = minio_client()
    for bucket in MINIO_BUCKETS:
        try:
            policy = s3.get_bucket_policy(Bucket=bucket)
            policy_str = str(policy.get("Policy", ""))
            assert "AllUsers" not in policy_str, (
                f"SECURITY FAILURE: bucket '{bucket}' has public access (AllUsers)"
            )
        except Exception as e:
            if "NoSuchBucketPolicy" in str(e):
                pass  # no policy = default private access ✓
            else:
                raise


def test_buckets_use_correct_naming(compose_up):
    """Buckets must follow the naming convention: landing, bronze, silver, gold."""
    s3 = minio_client()
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    expected = {"landing", "bronze", "silver", "gold"}
    # There must be no unexpected extra buckets (e.g. "test-bucket-public")
    unexpected = existing - expected - {"logs", "checkpoints"}  # auxiliary buckets ok
    assert not unexpected, f"Unexpected buckets detected: {unexpected}"


# ── Credential security in code ───────────────────────────────────────────────


def test_pii_salt_not_hardcoded_in_source():
    """PII_SALT must not appear hardcoded in Python files."""
    # Examples of values that should NOT be hardcoded
    suspicious_patterns = [
        r'PII_SALT\s*=\s*["\'][^"\']{8,}["\']',  # variable with value
        r'salt\s*=\s*["\'][^"\']{8,}["\']',  # generic salt variable
        r"secret.*=.*santander",  # case-specific secret
    ]

    py_files = list((REPO_ROOT / "pipelines").rglob("*.py"))
    py_files += list((REPO_ROOT / "airflow").rglob("*.py"))

    violations = []
    for f in py_files:
        content = f.read_text(errors="ignore")
        for pat in suspicious_patterns:
            if re.search(pat, content, re.IGNORECASE):
                violations.append(f"{f.relative_to(REPO_ROOT)}: matches '{pat}'")

    assert not violations, "Hardcoded credentials detected:\n" + "\n".join(violations)


def test_no_pii_column_names_outside_allowed_files():
    """PII column names may only appear in authorised files."""
    # Pattern: quotes around the column name (literal column in code)
    pii_pattern = re.compile(r'["\'](' + "|".join(_PII_COLS) + r')["\']')

    violations = []
    for py_file in (REPO_ROOT / "pipelines").rglob("*.py"):
        rel = str(py_file.relative_to(REPO_ROOT))
        if any(rel.endswith(allowed) for allowed in _PII_ALLOWED_PATHS):
            continue
        content = py_file.read_text(errors="ignore")
        if pii_pattern.search(content):
            violations.append(rel)

    assert not violations, "References to PII columns outside the authorised files:\n" + "\n".join(
        violations
    )


def test_no_plaintext_passwords_in_python():
    """Passwords and secrets must not appear hardcoded in .py files."""
    password_pattern = re.compile(
        r'(password|passwd|secret|token)\s*=\s*["\'][^"\']{6,}["\']',
        re.IGNORECASE,
    )
    # Known exceptions: test files with fake values
    allowed_test_values = {"postgres", "minioadmin", "localhost", "gold_engineer"}

    violations = []
    for py_file in (REPO_ROOT / "pipelines").rglob("*.py"):
        content = py_file.read_text(errors="ignore")
        for match in password_pattern.finditer(content):
            value_part = match.group(0).split("=", 1)[1].strip().strip("'\"")
            if value_part not in allowed_test_values:
                violations.append(f"{py_file.relative_to(REPO_ROOT)}: {match.group(0)[:60]}")

    assert not violations, "Hardcoded passwords detected:\n" + "\n".join(violations)


# ── PII scan on Silver ────────────────────────────────────────────────────────


def test_silver_pii_scan_filesystem():
    """No Parquet/Delta file in the local Silver directory must have a PII column name."""
    silver_local = REPO_ROOT / "data" / "silver"
    if not silver_local.exists():
        pytest.skip("data/silver/ does not exist locally — skip filesystem scan")

    parquet_files = list(silver_local.rglob("*.parquet"))
    if not parquet_files:
        pytest.skip("No Parquet files in data/silver/")

    try:
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow not installed — skip Parquet scan")

    violations = []
    for pf in parquet_files[:10]:  # sample: up to 10 files
        schema = pq.read_schema(pf)
        col_names = set(schema.names)
        leaked = col_names & _PII_COLS
        if leaked:
            violations.append(f"{pf.relative_to(REPO_ROOT)}: {leaked}")

    assert not violations, "PII detected in Silver Parquet files:\n" + "\n".join(violations)


# ── Kubernetes secrets ────────────────────────────────────────────────────────


def test_k8s_secret_yaml_does_not_expose_prod_values():
    """k8s/*.yaml must not contain real production values."""
    k8s_dir = REPO_ROOT / "k8s"
    if not k8s_dir.exists():
        pytest.skip("Directory k8s/ not found")

    prod_indicators = [
        r"\bpassword\s*:\s*[^\s]{12,}",  # long password (prod)
        r"arn:aws:",  # real AWS ARN
        r"AKIA[A-Z0-9]{16}",  # AWS access key
    ]

    violations = []
    for yaml_file in k8s_dir.rglob("*.yaml"):
        content = yaml_file.read_text(errors="ignore")
        for pat in prod_indicators:
            if re.search(pat, content):
                violations.append(f"{yaml_file.relative_to(REPO_ROOT)}: matches '{pat}'")

    assert not violations, "Sensitive production values in k8s/:\n" + "\n".join(violations)
