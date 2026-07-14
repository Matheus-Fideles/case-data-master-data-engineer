"""JDBC connection factory for Postgres — mirrors common/spark.py.

Single place that reads credentials from the environment for Gold DW access.
"""

from __future__ import annotations

import os


def make_pg_connection() -> tuple[str, dict]:
    """Returns (jdbc_url, properties) ready for df.write.jdbc(...)."""
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
