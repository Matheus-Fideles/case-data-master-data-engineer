"""Fábrica de conexão JDBC com Postgres — espelho de common/spark.py.

Único lugar que lê credenciais do ambiente para acesso ao DW Gold.
"""
from __future__ import annotations

import os


def make_pg_connection() -> tuple[str, dict]:
    """Retorna (jdbc_url, properties) prontos para df.write.jdbc(...)."""
    url = os.environ.get(
        "POSTGRES_JDBC_URL",
        "jdbc:postgresql://host.docker.internal:5432/postgres",
    )
    props = {
        "user": os.environ.get("POSTGRES_USER", "gold_engineer"),
        "password": os.environ.get("POSTGRES_PASSWORD", "gold_engineer"),
        "driver": "org.postgresql.Driver",
        "currentSchema": "gold_dw",
    }
    return url, props
