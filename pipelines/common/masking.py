"""Utilitários de mascaramento de PII conforme LGPD."""
from __future__ import annotations

import hashlib
import os

_SALT = os.getenv("PII_SALT", "santander-case-2024")


def mask_cpf(cpf: str | None) -> str | None:
    """SHA-256 + salt de um CPF string. Retorna None para entrada nula."""
    if not cpf:
        return None
    value = f"{_SALT}:{cpf.strip()}"
    return hashlib.sha256(value.encode()).hexdigest()


def mask_paciente(df, cpf_col: str = "cpf", output_col: str = "id_paciente_hash"):
    """Substitui coluna de CPF por hash SHA-256 em um DataFrame PySpark."""
    from pyspark.sql import functions as F

    salt = _SALT
    return df.withColumn(
        output_col,
        F.sha2(F.concat(F.lit(f"{salt}:"), F.col(cpf_col)), 256),
    ).drop(cpf_col)


def validate_no_pii(df, pii_cols: list[str] | None = None) -> None:
    """Levanta ValueError se qualquer coluna PII conhecida existir no DataFrame."""
    pii_cols = pii_cols or ["cpf", "rg", "nome_completo", "data_nascimento", "telefone", "email_pessoal"]
    found = [c for c in df.columns if c.lower() in pii_cols]
    if found:
        raise ValueError(f"DataFrame contém colunas PII não mascaradas: {found}")
