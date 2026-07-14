"""PII masking utilities compliant with LGPD."""
from __future__ import annotations

import hashlib
import os

_SALT = os.getenv("PII_SALT", "santander-case-2024")


def mask_cpf(cpf: str | None) -> str | None:
    """SHA-256 + salt of a CPF string. Returns None for null input."""
    if not cpf:
        return None
    value = f"{_SALT}:{cpf.strip()}"
    return hashlib.sha256(value.encode()).hexdigest()


def mask_paciente(df, cpf_col: str = "cpf", output_col: str = "id_paciente_hash"):
    """Replaces the CPF column with a SHA-256 hash in a PySpark DataFrame."""
    from pyspark.sql import functions as F

    salt = _SALT
    return df.withColumn(
        output_col,
        F.sha2(F.concat(F.lit(f"{salt}:"), F.col(cpf_col)), 256),
    ).drop(cpf_col)


def validate_no_pii(df, pii_cols: list[str] | None = None) -> None:
    """Raises ValueError if any known PII column exists in the DataFrame."""
    pii_cols = pii_cols or ["cpf", "rg", "nome_completo", "data_nascimento", "telefone", "email_pessoal"]
    found = [c for c in df.columns if c.lower() in pii_cols]
    if found:
        raise ValueError(f"DataFrame contains unmasked PII columns: {found}")
